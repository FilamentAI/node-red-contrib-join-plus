module.exports = function (RED) {
    function JoinNode(n) {
        RED.nodes.createNode(this, n);
        this.mode = n.mode || "auto";
        this.property = n.property || "payload";
        this.propertyType = n.propertyType || "msg";
        if (this.propertyType === 'full') {
            this.property = "payload";
        }
        this.key = n.key || "topic";
        this.timer = (this.mode === "auto") ? 0 : Number(n.timeout || 0) * 1000;
        this.unique = n.unique || false; // AF
        this.count = Number(n.count || 0);
        this.joiner = n.joiner || "";
        this.joinerType = n.joinerType || "str";

        this.reduce = (this.mode === "reduce");
        if (this.reduce) {
            this.exp_init = n.reduceInit;
            this.exp_init_type = n.reduceInitType;
            var exp_reduce = n.reduceExp;
            var exp_fixup = exp_or_undefined(n.reduceFixup);
            this.reduce_right = n.reduceRight;
            try {
                this.reduce_exp = RED.util.prepareJSONataExpression(exp_reduce, this);
                this.reduce_fixup = (exp_fixup !== undefined) ? RED.util.prepareJSONataExpression(exp_fixup, this) : undefined;
            } catch (e) {
                this.error(RED._("join.errors.invalid-expr", {
                    error: e.message
                }));
            }
        }

        if (this.joinerType === "str") {
            this.joiner = this.joiner.replace(/\\n/g, "\n").replace(/\\r/g, "\r").replace(/\\t/g, "\t").replace(/\\e/g, "\e").replace(/\\f/g, "\f").replace(/\\0/g, "\0");
        } else if (this.joinerType === "bin") {
            var joinArray = JSON.parse(n.joiner)
            if (Array.isArray(joinArray)) {
                this.joiner = Buffer.from(joinArray);
            } else {
                throw new Error("not an array");
            }
        }

        this.build = n.build || "array";
        this.accumulate = n.accumulate || "false";

        this.topics = (n.topics || []).map(function (x) {
            return x.topic;
        });
        this.merge_on_change = n.mergeOnChange || false;
        this.topic_counts = undefined;
        this.output = n.output || "stream";
        this.pending = {};
        this.pending_count = 0;

        //this.topic = n.topic;
        var node = this;
        var inflight = {};
        var prior = []; // AF

        var completeSend = function (partId) {
            var group = inflight[partId];
            prior.push ( partId ); // AF
            clearTimeout(group.timeout);
            if ((node.accumulate !== true) || group.msg.hasOwnProperty("complete")) {
                delete inflight[partId];
            }
            if (group.type === 'array' && group.arrayLen > 1) {
                var newArray = [];
                group.payload.forEach(function (n) {
                    newArray = newArray.concat(n);
                })
                group.payload = newArray;
            } else if (group.type === 'buffer') {
                var buffers = [];
                var bufferLen = 0;
                if (group.joinChar !== undefined) {
                    var joinBuffer = Buffer.from(group.joinChar);
                    for (var i = 0; i < group.payload.length; i++) {
                        if (i > 0) {
                            buffers.push(joinBuffer);
                            bufferLen += joinBuffer.length;
                        }
                        buffers.push(group.payload[i]);
                        bufferLen += group.payload[i].length;
                    }
                } else {
                    bufferLen = group.bufferLen;
                    buffers = group.payload;
                }
                group.payload = Buffer.concat(buffers, bufferLen);
            }

            if (group.type === 'string') {
                var groupJoinChar = group.joinChar;
                if (typeof group.joinChar !== 'string') {
                    groupJoinChar = group.joinChar.toString();
                }
                RED.util.setMessageProperty(group.msg, node.property, group.payload.join(groupJoinChar));
            } else {
                if (node.propertyType === 'full') {
                    group.msg = RED.util.cloneMessage(group.msg);
                }
                RED.util.setMessageProperty(group.msg, node.property, group.payload);
            }
            if (group.msg.hasOwnProperty('parts') && group.msg.parts.hasOwnProperty('parts')) {
                group.msg.parts = group.msg.parts.parts;
            } else {
                delete group.msg.parts;
            }
            delete group.msg.complete;
            node.send(group.msg);
        }

        this.on("input", function (msg) {
            try {
                var property;
                if (node.mode === 'auto' && (!msg.hasOwnProperty("parts") || !msg.parts.hasOwnProperty("id"))) {
                    node.warn("Message missing msg.parts property - cannot join in 'auto' mode")
                    return;
                }

                if (node.propertyType == "full") {
                    property = msg;
                } else {
                    try {
                        property = RED.util.getMessageProperty(msg, node.property);
                    } catch (err) {
                        node.warn("Message property " + node.property + " not found");
                        return;
                    }
                }

                var partId;
                var payloadType;
                var propertyKey;
                var targetCount;
                var joinChar;
                var arrayLen;
                var propertyIndex;
                if (node.mode === "auto") {
                    // Use msg.parts to identify all of the group information
                    partId = msg.parts.id;
                    payloadType = msg.parts.type;
                    targetCount = msg.parts.count;
                    joinChar = msg.parts.ch;
                    propertyKey = msg.parts.key;
                    arrayLen = msg.parts.len;
                    propertyIndex = msg.parts.index;
                    node.timer = Number( msg.parts.timeout || 0) * 1000; // AF
                    node.unique = msg.parts.unique || node.unique; // AF
                } else if (node.mode === 'reduce') {
                    reduce_msg(node, msg);
                    return;
                } else {
                    // Use the node configuration to identify all of the group information
                    partId = "_";
                    payloadType = node.build;
                    targetCount = node.count;
                    joinChar = node.joiner;
                    if (targetCount === 0 && msg.hasOwnProperty('parts')) {
                        targetCount = msg.parts.count || 0;
                    }
                    if (node.build === 'object') {
                        propertyKey = RED.util.getMessageProperty(msg, node.key);
                    }
                }

                if ((payloadType === 'object') && (propertyKey === null || propertyKey === undefined || propertyKey === "")) {
                    if (node.mode === "auto") {
                        node.warn("Message missing 'msg.parts.key' property - cannot add to object");
                    } else {
                        if (msg.hasOwnProperty('complete')) {
                            completeSend(partId);
                        } else {
                            node.warn("Message missing key property 'msg." + node.key + "' - cannot add to object")
                        }
                    }
                    return;
                }
                
                if ( node.unique && prior.indexOf(partId) >= 0 ) { // AF
                    return; // Seen & sent on this ID before.
                }
                
                if (!inflight.hasOwnProperty(partId)) {
                    if (payloadType === 'object' || payloadType === 'merged') {
                        inflight[partId] = {
                            currentCount: 0,
                            payload: {},
                            targetCount: targetCount,
                            type: "object",
                            msg: msg
                        };
                    } else if (node.accumulate === true) {
                        if (msg.hasOwnProperty("reset")) {
                            delete inflight[partId];
                        }
                        inflight[partId] = inflight[partId] || {
                            currentCount: 0,
                            payload: {},
                            targetCount: targetCount,
                            type: payloadType,
                            msg: msg
                        }
                        if (payloadType === 'string' || payloadType === 'array' || payloadType === 'buffer') {
                            inflight[partId].payload = [];
                        }
                    } else {
                        inflight[partId] = {
                            currentCount: 0,
                            payload: [],
                            targetCount: targetCount,
                            type: payloadType,
                            msg: msg
                        };
                        if (payloadType === 'string') {
                            inflight[partId].joinChar = joinChar;
                        } else if (payloadType === 'array') {
                            inflight[partId].arrayLen = arrayLen;
                        } else if (payloadType === 'buffer') {
                            inflight[partId].bufferLen = 0;
                            inflight[partId].joinChar = joinChar;
                        }
                    }
                    if (node.timer > 0) {
                        inflight[partId].timeout = setTimeout(function () {
                            completeSend(partId)
                        }, node.timer)
                    }
                }

                var group = inflight[partId];
                if (payloadType === 'buffer') {
                    inflight[partId].bufferLen += property.length;
                }
                if (payloadType === 'object') {
                    group.payload[propertyKey] = property;
                    group.currentCount = Object.keys(group.payload).length;
                    //msg.topic = node.topic || msg.topic;
                } else if (payloadType === 'merged') {
                    if (Array.isArray(property) || typeof property !== 'object') {
                        if (!msg.hasOwnProperty("complete")) {
                            node.warn("Cannot merge non-object types");
                        }
                    } else {
                        for (propertyKey in property) {
                            if (property.hasOwnProperty(propertyKey) && propertyKey !== '_msgid') {
                                group.payload[propertyKey] = property[propertyKey];
                            }
                        }
                        group.currentCount = Object.keys(group.payload).length;
                        //group.currentCount++;
                    }
                } else {
                    if (!isNaN(propertyIndex)) {
                        group.payload[propertyIndex] = property;
                    } else {
                        group.payload.push(property);
                    }
                    group.currentCount++;
                }
                // TODO: currently reuse the last received - add option to pick first received
                group.msg = msg;
                var tcnt = group.targetCount;
                if (msg.hasOwnProperty("parts")) {
                    tcnt = group.targetCount || msg.parts.count;
                }
                if ((tcnt > 0 && group.currentCount >= tcnt) || msg.hasOwnProperty('complete')) {
                    completeSend(partId);
                }
            } catch (err) {
                console.log(err.stack);
            }
        });

        this.on("close", function () {
            for (var i in inflight) {
                if (inflight.hasOwnProperty(i)) {
                    clearTimeout(inflight[i].timeout);
                }
            }
        });
    }
    RED.nodes.registerType("assemble", JoinNode);
}
