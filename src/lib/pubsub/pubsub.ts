const OpStream = require('../op-stream');
const util = require('../util');
const ShareDBError = require('../error');

export interface IPubSubOptions {
    prefix?: string

}

export interface IStream {
    id: string
}


export abstract class PubSub {

    private static shallowCopy(object: object): object {
        const out = {};
        for (const key in object) {
            out[key] = object[key];
        }
        return out;
    }

    protected prefix?: string;
    protected nextStreamId: number = 1;
    protected streamsCount: number = 0;
    // Maps channel -> id -> stream
    protected streams: { [channel: string]: IStream } = {};
    // State for tracking subscriptions. We track this.subscribed separately from
    // the streams, since the stream gets added synchronously, and the subscribe
    // isn't complete until the callback returns from Redis
    // Maps channel -> true
    protected subscribed: { [channel: string]: boolean } = {};



    protected constructor(options?: IPubSubOptions) {
        this.prefix = options && options.prefix;
    }

    public close(callback: () => void) {
        for (const channel in this.streams) {
            const map = this.streams[channel];
            for (const id in map) {
                map[id].destroy();
            }
        }
        if (callback) callback();
    };

    public publish(channels: string[], data: object, callback: (err: Error | null) => void): void {
        if (this.prefix) {
            for (let i = 0; i < channels.length; i++) {
                channels[i] = this.prefix + ' ' + channels[i];
            }
        }
        this._publish(channels, data, callback);
    };

    public subscribe(channel:string, callback: (err: Error | null, stream?: IStream) => void) {
        if (this.prefix) {
            channel = this.prefix + ' ' + channel;
        }

        const pubsub = this;
        if (this.subscribed[channel]) {
            process.nextTick(function () {
                const stream = pubsub._createStream(channel);
                callback(null, stream);
            });
            return;
        }
        this._subscribe(channel, function (err) {
            if (err) return callback(err);
            pubsub.subscribed[channel] = true;
            const stream = pubsub._createStream(channel);
            callback(null, stream);
        });
    };

    protected abstract _subscribe(channel: string, callback: (err: Error | null) => void): void;

    protected abstract _unsubscribe(channel: string, callback: (err: Error | null) => void): void;

    protected abstract _publish(channels: string[], data: Object, callback: (err: Error | null) => void): void;

    protected _emit(channel: string, data: object) {
        const channelStreams = this.streams[channel];
        if (channelStreams) {
            for (const id in channelStreams) {
                const copy = PubSub.shallowCopy(data);
                channelStreams[id].pushOp(copy["c"], copy["d"], copy);
            }
        }
    };

    private _createStream(channel: string): IStream {
        const stream = new OpStream();
        const pubsub = this;
        stream.once('close', function () {
            pubsub._removeStream(channel, stream);
        });

        this.streamsCount++;
        const map = this.streams[channel] || (this.streams[channel] = {} as any);
        stream.id = this.nextStreamId++;
        map[stream.id] = stream;

        return stream;
    };

    private _removeStream(channel: string, stream: IStream) {
        const map = this.streams[channel];
        if (!map) return;

        this.streamsCount--;
        delete map[stream.id];

        // Cleanup if this was the last subscribed stream for the channel
        if (util.hasKeys(map)) return;
        delete this.streams[channel];
        // Synchronously clear subscribed state. We won't actually be unsubscribed
        // until some unknown time in the future. If subscribe is called in this
        // period, we want to send a subscription message and wait for it to
        // complete before we can count on being subscribed again
        delete this.subscribed[channel];

        this._unsubscribe(channel, function (err) {
            if (err) throw err;
        });
    };

}