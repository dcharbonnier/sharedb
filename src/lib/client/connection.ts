import {EventEmitter} from "events";

var Doc = require('./doc');
var Query = require('./query');
var ShareDBError = require('../error');
var types = require('../types');
var util = require('../util');


/**
 * Handles communication with the sharejs server and provides queries and
 * documents.
 *
 * We create a connection with a socket object
 *   connection = new sharejs.Connection(sockset)
 * The socket may be any object handling the websocket protocol. See the
 * documentation of bindToSocket() for details. We then wait for the connection
 * to connect
 *   connection.on('connected', ...)
 * and are finally able to work with shared documents
 *   connection.get('food', 'steak') // Doc
 *
 * @param socket @see bindToSocket
 */

class Connection extends EventEmitter {

    private static connectionState(socket) {
        if (socket.readyState === 0 || socket.readyState === 1) return 'connecting';
        return 'disconnected';
    }

    public collections: any;
    public nextQueryId: any;
    public queries: any;
    public seq: any;
    public id: any;
    public agent: any;
    public debug: any;
    public state: any;
    public socket: any;
    public canSend: any;
    public bulk: any;

    constructor(socket) {
        super();
        // Map of collection -> id -> doc object for created documents.
        // (created documents MUST BE UNIQUE)
        this.collections = {};

        // Each query is created with an id that the server uses when it sends us
        // info about the query (updates, etc)
        this.nextQueryId = 1;

        // Map from query ID -> query object.
        this.queries = {};

        // A unique message number for the given id
        this.seq = 1;

        // Equals agent.clientId on the server
        this.id = null;

        // This direct reference from connection to agent is not used internal to
        // ShareDB, but it is handy for server-side only user code that may cache
        // state on the agent and read it in middleware
        this.agent = null;

        this.debug = false;

        this.state = Connection.connectionState(socket);

        this.bindToSocket(socket);
    }


    /**
     * Use socket to communicate with server
     *
     * Socket is an object that can handle the websocket protocol. This method
     * installs the onopen, onclose, onmessage and onerror handlers on the socket to
     * handle communication and sends messages by calling socket.send(message). The
     * sockets `readyState` property is used to determine the initaial state.
     *
     * @param socket Handles the websocket protocol
     * @param socket.readyState
     * @param socket.close
     * @param socket.send
     * @param socket.onopen
     * @param socket.onclose
     * @param socket.onmessage
     * @param socket.onerror
     */
    public bindToSocket(socket) {
        if (this.socket) {
            this.socket.close();
            this.socket.onmessage = null;
            this.socket.onopen = null;
            this.socket.onerror = null;
            this.socket.onclose = null;
        }

        this.socket = socket;

        // State of the connection. The corresponding events are emitted when this changes
        //
        // - 'connecting'   The connection is still being established, or we are still
        //                    waiting on the server to send us the initialization message
        // - 'connected'    The connection is open and we have connected to a server
        //                    and recieved the initialization message
        // - 'disconnected' Connection is closed, but it will reconnect automatically
        // - 'closed'       The connection was closed by the client, and will not reconnect
        // - 'stopped'      The connection was closed by the server, and will not reconnect
        var newState = Connection.connectionState(socket);
        this._setState(newState);

        // This is a helper variable the document uses to see whether we're
        // currently in a 'live' state. It is true if and only if we're connected
        this.canSend = false;

        var connection = this;

        socket.onmessage = function (event) {
            try {
                var data = (typeof event.data === 'string') ?
                    JSON.parse(event.data) : event.data;
            } catch (err) {
                console.warn('Failed to parse message', event);
                return;
            }

            if (connection.debug) console.log('RECV', JSON.stringify(data));

            var request = {data: data};
            connection.emit('receive', request);
            if (!request.data) return;

            try {
                connection.handleMessage(request.data);
            } catch (err) {
                process.nextTick(function () {
                    connection.emit('error', err);
                });
            }
        };

        socket.onopen = function () {
            connection._setState('connecting');
        };

        socket.onerror = function (err) {
            // This isn't the same as a regular error, because it will happen normally
            // from time to time. Your connection should probably automatically
            // reconnect anyway, but that should be triggered off onclose not onerror.
            // (onclose happens when onerror gets called anyway).
            connection.emit('connection error', err);
        };

        socket.onclose = function (reason) {
            // node-browserchannel reason values:
            //   'Closed' - The socket was manually closed by calling socket.close()
            //   'Stopped by server' - The server sent the stop message to tell the client not to try connecting
            //   'Request failed' - Server didn't respond to request (temporary, usually offline)
            //   'Unknown session ID' - Server session for client is missing (temporary, will immediately reestablish)

            if (reason === 'closed' || reason === 'Closed') {
                connection._setState('closed', reason);

            } else if (reason === 'stopped' || reason === 'Stopped by server') {
                connection._setState('stopped', reason);

            } else {
                connection._setState('disconnected', reason);
            }
        };
    };

    /**
     * @param {object} message
     * @param {String} message.a action
     */
    public handleMessage(message) {
        var err:any = null;
        if (message.error) {
            // wrap in Error object so can be passed through event emitters
            err = new Error(message.error.message);
            err.code = message.error.code;
            // Add the message data to the error object for more context
            err.data = message;
            delete message.error;
        }
        // Switch on the message action. Most messages are for documents and are
        // handled in the doc class.
        switch (message.a) {
            case 'init':
                // Client initialization packet
                if (message.protocol !== 1) {
                    err = new ShareDBError(4019, 'Invalid protocol version');
                    return this.emit('error', err);
                }
                if (types.map[message.type] !== types.defaultType) {
                    err = new ShareDBError(4020, 'Invalid default type');
                    return this.emit('error', err);
                }
                if (typeof message.id !== 'string') {
                    err = new ShareDBError(4021, 'Invalid client id');
                    return this.emit('error', err);
                }
                this.id = message.id;

                this._setState('connected');
                return;

            case 'qf':
                var query = this.queries[message.id];
                if (query) query._handleFetch(err, message.data, message.extra);
                return;
            case 'qs':
                var query = this.queries[message.id];
                if (query) query._handleSubscribe(err, message.data, message.extra);
                return;
            case 'qu':
                // Queries are removed immediately on calls to destroy, so we ignore
                // replies to query unsubscribes. Perhaps there should be a callback for
                // destroy, but this is currently unimplemented
                return;
            case 'q':
                // Query message. Pass this to the appropriate query object.
                var query = this.queries[message.id];
                if (!query) return;
                if (err) return query._handleError(err);
                if (message.diff) query._handleDiff(message.diff);
                if (message.hasOwnProperty('extra')) query._handleExtra(message.extra);
                return;

            case 'bf':
                return this._handleBulkMessage(message, '_handleFetch');
            case 'bs':
                return this._handleBulkMessage(message, '_handleSubscribe');
            case 'bu':
                return this._handleBulkMessage(message, '_handleUnsubscribe');

            case 'f':
                var doc = this.getExisting(message.c, message.d);
                if (doc) doc._handleFetch(err, message.data);
                return;
            case 's':
                var doc = this.getExisting(message.c, message.d);
                if (doc) doc._handleSubscribe(err, message.data);
                return;
            case 'u':
                var doc = this.getExisting(message.c, message.d);
                if (doc) doc._handleUnsubscribe(err);
                return;
            case 'op':
                var doc = this.getExisting(message.c, message.d);
                if (doc) doc._handleOp(err, message);
                return;

            default:
                console.warn('Ignoring unrecognized message', message);
        }
    };

    public _handleBulkMessage(message, method) {
        if (message.data) {
            for (let id in message.data) {
                let doc = this.getExisting(message.c, id);
                if (doc) doc[method](message.error, message.data[id]);
            }
        } else if (Array.isArray(message.b)) {
            for (let i = 0; i < message.b.length; i++) {
                let id = message.b[i];
                let doc = this.getExisting(message.c, id);
                if (doc) doc[method](message.error);
            }
        } else if (message.b) {
            for (let id in message.b) {
                let doc = this.getExisting(message.c, id);
                if (doc) doc[method](message.error);
            }
        } else {
            console.error('Invalid bulk message', message);
        }
    };

    public _reset() {
        this.seq = 1;
        this.id = null;
        this.agent = null;
    };

// Set the connection's state. The connection is basically a state machine.
    public _setState(newState, reason?) {
        if (this.state === newState) return;

        // I made a state diagram. The only invalid transitions are getting to
        // 'connecting' from anywhere other than 'disconnected' and getting to
        // 'connected' from anywhere other than 'connecting'.
        if (
            (newState === 'connecting' && this.state !== 'disconnected' && this.state !== 'stopped' && this.state !== 'closed') ||
            (newState === 'connected' && this.state !== 'connecting')
        ) {
            var err = new ShareDBError(5007, 'Cannot transition directly from ' + this.state + ' to ' + newState);
            return this.emit('error', err);
        }

        this.state = newState;
        this.canSend = (newState === 'connected');

        if (newState === 'disconnected' || newState === 'stopped' || newState === 'closed') this._reset();

        // Group subscribes together to help server make more efficient calls
        this.startBulk();
        // Emit the event to all queries
        for (var id in this.queries) {
            var query = this.queries[id];
            query._onConnectionStateChanged();
        }
        // Emit the event to all documents
        for (var collection in this.collections) {
            var docs = this.collections[collection];
            for (var id in docs) {
                docs[id]._onConnectionStateChanged();
            }
        }
        this.endBulk();

        this.emit(newState, reason);
        this.emit('state', newState, reason);
    };

    public startBulk() {
        if (!this.bulk) this.bulk = {};
    };

    public endBulk() {
        if (this.bulk) {
            for (var collection in this.bulk) {
                var actions = this.bulk[collection];
                this._sendBulk('f', collection, actions.f);
                this._sendBulk('s', collection, actions.s);
                this._sendBulk('u', collection, actions.u);
            }
        }
        this.bulk = null;
    };

    public _sendBulk(action, collection, values) {
        if (!values) return;
        var ids:any = [];
        var versions = {};
        var versionsCount = 0;
        var versionId;
        for (let id in values) {
            var value = values[id];
            if (value == null) {
                ids.push(id);
            } else {
                versions[id] = value;
                versionId = id;
                versionsCount++;
            }
        }
        if (ids.length === 1) {
            let id = ids[0];
            this.send({a: action, c: collection, d: id});
        } else if (ids.length) {
            this.send({a: 'b' + action, c: collection, b: ids});
        }
        if (versionsCount === 1) {
            var version = versions[versionId];
            this.send({a: action, c: collection, d: versionId, v: version});
        } else if (versionsCount) {
            this.send({a: 'b' + action, c: collection, b: versions});
        }
    };

    public _sendAction(action, doc, version?) {
        // Ensure the doc is registered so that it receives the reply message
        this._addDoc(doc);
        if (this.bulk) {
            // Bulk subscribe
            var actions = this.bulk[doc.collection] || (this.bulk[doc.collection] = {});
            var versions = actions[action] || (actions[action] = {});
            var isDuplicate = versions.hasOwnProperty(doc.id);
            versions[doc.id] = version;
            return isDuplicate;
        } else {
            // Send single doc subscribe message
            var message = {a: action, c: doc.collection, d: doc.id, v: version};
            this.send(message);
        }
    };

    public sendFetch(doc) {
        return this._sendAction('f', doc, doc.version);
    };

    public sendSubscribe(doc) {
        return this._sendAction('s', doc, doc.version);
    };

    public sendUnsubscribe(doc) {
        return this._sendAction('u', doc);
    };

    public sendOp(doc, op) {
        // Ensure the doc is registered so that it receives the reply message
        this._addDoc(doc);
        var message: any = {
            a: 'op',
            c: doc.collection,
            d: doc.id,
            v: doc.version,
            src: op.src,
            seq: op.seq
        };
        if (op.op) message.op = op.op;
        if (op.create) message.create = op.create;
        if (op.del) message.del = op.del;
        this.send(message);
    };


    /**
     * Sends a message down the socket
     */
    public send(message) {
        if (this.debug) console.log('SEND', JSON.stringify(message));

        this.emit('send', message);
        this.socket.send(JSON.stringify(message));
    };


    /**
     * Closes the socket and emits 'closed'
     */
    public close() {
        this.socket.close();
    };

    public getExisting(collection, id) {
        if (this.collections[collection]) return this.collections[collection][id];
    };


    /**
     * Get or create a document.
     *
     * @param collection
     * @param id
     * @return {Doc}
     */
    public get(collection, id) {
        var docs = this.collections[collection] ||
            (this.collections[collection] = {});

        var doc = docs[id];
        if (!doc) {
            doc = docs[id] = new Doc(this, collection, id);
            this.emit('doc', doc);
        }

        return doc;
    };


    /**
     * Remove document from this.collections
     *
     * @private
     */
    public _destroyDoc(doc) {
        var docs = this.collections[doc.collection];
        if (!docs) return;

        delete docs[doc.id];

        // Delete the collection container if its empty. This could be a source of
        // memory leaks if you slowly make a billion collections, which you probably
        // won't do anyway, but whatever.
        if (!util.hasKeys(docs)) {
            delete this.collections[doc.collection];
        }
    };

    public _addDoc(doc) {
        var docs = this.collections[doc.collection];
        if (!docs) {
            docs = this.collections[doc.collection] = {};
        }
        if (docs[doc.id] !== doc) {
            docs[doc.id] = doc;
        }
    };

// Helper for createFetchQuery and createSubscribeQuery, below.
    public _createQuery(action, collection, q, options, callback) {
        var id = this.nextQueryId++;
        var query = new Query(action, this, id, collection, q, options, callback);
        this.queries[id] = query;
        query.send();
        return query;
    };

// Internal function. Use query.destroy() to remove queries.
    public _destroyQuery(query) {
        delete this.queries[query.id];
    };

// The query options object can contain the following fields:
//
// db: Name of the db for the query. You can attach extraDbs to ShareDB and
//   pick which one the query should hit using this parameter.

// Create a fetch query. Fetch queries are only issued once, returning the
// results directly into the callback.
//
// The callback should have the signature function(error, results, extra)
// where results is a list of Doc objects.
    public createFetchQuery(collection, q, options, callback) {
        return this._createQuery('qf', collection, q, options, callback);
    };

// Create a subscribe query. Subscribe queries return with the initial data
// through the callback, then update themselves whenever the query result set
// changes via their own event emitter.
//
// If present, the callback should have the signature function(error, results, extra)
// where results is a list of Doc objects.
    public createSubscribeQuery(collection, q, options, callback) {
        return this._createQuery('qs', collection, q, options, callback);
    };

    public hasPending() {
        return !!(
            this._firstDoc(Connection.hasPending) ||
            this._firstQuery(Connection.hasPending)
        );
    };

    private static hasPending(object) {
        return object.hasPending();
    }

    public hasWritePending() {
        return !!this._firstDoc(Connection.hasWritePending);
    };

    private static hasWritePending(object) {
        return object.hasWritePending();
    }

    public whenNothingPending(callback) {
        var doc = this._firstDoc(Connection.hasPending);
        if (doc) {
            // If a document is found with a pending operation, wait for it to emit
            // that nothing is pending anymore, and then recheck all documents again.
            // We have to recheck all documents, just in case another mutation has
            // been made in the meantime as a result of an event callback
            doc.once('nothing pending', this._nothingPendingRetry(callback));
            return;
        }
        var query = this._firstQuery(Connection.hasPending);
        if (query) {
            query.once('ready', this._nothingPendingRetry(callback));
            return;
        }
        // Call back when no pending operations
        process.nextTick(callback);
    };

    public _nothingPendingRetry(callback) {
        var connection = this;
        return function () {
            process.nextTick(function () {
                connection.whenNothingPending(callback);
            });
        };
    };

    public _firstDoc(fn) {
        for (var collection in this.collections) {
            var docs = this.collections[collection];
            for (var id in docs) {
                var doc = docs[id];
                if (fn(doc)) {
                    return doc;
                }
            }
        }
    };

    public _firstQuery(fn) {
        for (var id in this.queries) {
            var query = this.queries[id];
            if (fn(query)) {
                return query;
            }
        }
    };
}

module.exports = Connection;
