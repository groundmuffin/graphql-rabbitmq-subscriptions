import { PubSubEngine } from 'graphql-subscriptions/dist/pubsub-engine';
import {
  RabbitMqConnectionFactory,
  RabbitMqPublisher,
  RabbitMqSubscriber,
  IRabbitMqConnectionConfig,
} from '@groundmuffin/rabbitmq-pub-sub';
import * as Logger from 'bunyan';
import { createChildLogger } from './child-logger';
import { PubSubAsyncIterator } from './pubsub-async-iterator';

export interface PubSubRabbitMQBusOptions {
  config?: IRabbitMqConnectionConfig;
  connectionListener?: (err: Error) => void;
  triggerTransform?: TriggerTransform;
  logger?: Logger;
}

export class AmqpPubSub implements PubSubEngine {

  constructor(options: PubSubRabbitMQBusOptions = {}) {

    this.triggerTransform = options.triggerTransform || (trigger => trigger as string);
    const config = options.config || { host: '127.0.0.1', port: 5672 };
    const { logger } = options;

    this.logger = createChildLogger(logger, 'AmqpPubSub');

    const factory = new RabbitMqConnectionFactory(logger, config);

    this.consumer = new RabbitMqSubscriber(logger, factory);
    this.producer = new RabbitMqPublisher(logger, factory);

    this.subscriptionMap = {};
    this.subsRefsMap = {};
    this.subsDisposerMap = {};
    this.currentSubscriptionId = 0;
  }

  public publish(trigger: string, payload: any): boolean {
    this.logger.trace("publishing for queue '%s' (%j)", trigger, payload);
    this.producer.publish(trigger, payload);
    return true;
  }

  public subscribe(trigger: string, onMessage: Function, options?: Object): Promise<number> {
    const triggerName: string = this.triggerTransform(trigger, options);
    const id = this.currentSubscriptionId++;
    this.subscriptionMap[id] = [triggerName, onMessage];

    let refs = this.subsRefsMap[triggerName];
    if (refs && refs.length > 0) {
      const newRefs = [...refs, id];
      this.subsRefsMap[triggerName] = newRefs;
      this.logger.trace("subscriber exist, adding triggerName '%s' to saved list.", triggerName);
      return Promise.resolve(id);

    } else {
      return new Promise<number>((resolve, reject) => {
        this.logger.trace("trying to subscribe to queue '%s'", triggerName);
        this.consumer.subscribe(triggerName, (msg) => this.onMessage(triggerName, msg))
          .then(disposer => {
              this.subsRefsMap[triggerName] = [...(refs || []), id];
              this.subsDisposerMap[triggerName] = disposer;
              return resolve(id);
          })
          .catch(err => {
              this.logger.error(err, "failed to recieve message from queue '%s'", triggerName);
              reject(id);
          });
      });
    }
  }

	public _subscribe(trigger: string, onMessage: Function, options?: Object): Promise<number> {
		const triggerName: string = this.triggerTransform(trigger, options);
		const id = this.currentSubscriptionId++;
		this.subscriptionMap[id] = [triggerName, onMessage];
		let refs = this.subsRefsMap[triggerName];
		if (refs && refs.length > 0) {
			const newRefs = [...refs, id];
			this.subsRefsMap[triggerName] = newRefs;
			this.logger.trace("subscriber exist, adding triggerName '%s' to saved list.", triggerName);
			return Promise.resolve(id);
		} else {
			return new Promise<any>((resolve, reject) => {
				this.logger.trace("trying to subscribe to queue '%s'", triggerName);
				this.consumer._subscribe(triggerName, (msg) => this.onMessage(triggerName, msg))
					.then(({disposer, channel}) => {
						this.subsRefsMap[triggerName] = [...(this.subsRefsMap[triggerName] || []), id];
						this.unsubscribeChannel = disposer;
						// return resolve(id);
						return resolve({
							id: id,
							channel: channel
						});
					}).catch(err => {
					this.logger.error(err, "failed to recieve message from queue '%s'", triggerName);
					reject(id);
				});
			});
		}
	}

  public unsubscribe(subId: number) {
    const [triggerName = null] = this.subscriptionMap[subId] || [];
    const refs = this.subsRefsMap[triggerName];

    if (!refs) {
      this.logger.error("There is no subscription of id '%s'", subId);
      throw new Error(`There is no subscription of id "{subId}"`);
    }

    let newRefs;
    if (refs.length === 1) {
      newRefs = [];
      const disposer = this.subsDisposerMap[triggerName];
      disposer().then(() => {
        this.logger.trace("cancelled channel from subscribing to queue '%s'", triggerName);
        delete this.subsRefsMap[triggerName];
      }).catch(err => {
        this.logger.error(err, "channel cancellation failed from queue '%j'", triggerName);
      });
    } else {
      const index = refs.indexOf(subId);
      const newRefs = index === -1 ? refs : [...refs.slice(0, index), ...refs.slice(index + 1)];
      this.subsRefsMap[triggerName] = newRefs;
      
      this.logger.trace("removing triggerName from listening '%s' ", triggerName);
    }
    delete this.subscriptionMap[subId];
    this.logger.trace("list of subscriptions still available '(%j)'", this.subscriptionMap);
  }

	public asyncIterator<T>(triggers: string | string[], options: any): AsyncIterator<T> {
		return new PubSubAsyncIterator<T>(this, triggers, options);
	}

  public getSubscriber(): RabbitMqSubscriber {
    return this.consumer;
  }

  public getPublisher(): RabbitMqPublisher {
    return this.producer;
  }
  
  private onMessage(channel: string, message: string) {
    const subscribers = this.subsRefsMap[channel];

    // Don't work for nothing..
    if (!subscribers || !subscribers.length) {
      return;
    }

    this.logger.trace("sending message to subscriber callback function '(%j)'", message);

    let parsedMessage;
    try {
      parsedMessage = JSON.parse(message);
    } catch (e) {
      parsedMessage = message;
    }

    for (const subId of subscribers) {
      const listener = this.subscriptionMap[subId][1];
      listener(parsedMessage);
    }
  }

  private triggerTransform: TriggerTransform;
  private consumer: RabbitMqSubscriber;
  private producer: RabbitMqPublisher;

  private subscriptionMap: { [subId: number]: [string, Function] };
  private subsRefsMap: { [trigger: string]: Array<number> };
  private subsDisposerMap: { [trigger: string]: Function };

  private currentSubscriptionId: number;
  
  private logger: Logger;
}

export type Path = Array<string | number>;
export type Trigger = string | Path;
export type TriggerTransform = (trigger: Trigger, channelOptions?: Object) => string;

