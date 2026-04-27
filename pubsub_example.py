#!/usr/bin/env python3
"""
Pub/Sub pattern built on top of antzkv SET/GET.

Uses a convention: channel keys are prefixed with 'channel:'.
Publisher writes channel:news = message
Subscribers poll channel:news every N ms.
"""
import sys, os, time, threading, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'clients', 'python'))
from antzkv import AntzKVClient

HOST = "127.0.0.1"
PORT = 6379
CHANNEL = "channel:news"


class Publisher:
    def __init__(self, host, port):
        self.client = AntzKVClient(host, port)
        self.client.connect()
        self.msg_id = 0

    def publish(self, message):
        self.msg_id += 1
        payload = json.dumps({"id": self.msg_id, "msg": message, "ts": time.time()})
        self.client.set(CHANNEL, payload)
        print(f"[PUB] -> {message}")

    def close(self):
        self.client.close()


class Subscriber:
    def __init__(self, host, port, name):
        self.client = AntzKVClient(host, port)
        self.client.connect()
        self.name = name
        self.last_id = -1
        self.running = False
        self.thread = None

    def _loop(self, interval_ms):
        while self.running:
            raw = self.client.get(CHANNEL)
            if raw and raw != "(nil)":
                try:
                    data = json.loads(raw)
                    msg_id = data.get("id", -1)
                    if msg_id != self.last_id:
                        self.last_id = msg_id
                        print(f"[{self.name}] <- {data['msg']} (id={msg_id})")
                except (json.JSONDecodeError, KeyError):
                    pass
            time.sleep(interval_ms / 1000.0)

    def start(self, interval_ms=100):
        self.running = True
        self.thread = threading.Thread(target=self._loop, args=(interval_ms,))
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        self.client.close()


def demo():
    pub = Publisher(HOST, PORT)
    sub1 = Subscriber(HOST, PORT, "SUB-1")
    sub2 = Subscriber(HOST, PORT, "SUB-2")

    sub1.start(interval_ms=200)
    sub2.start(interval_ms=200)

    time.sleep(0.3)
    pub.publish("Hello subscribers!")
    time.sleep(0.3)
    pub.publish("Second message")
    time.sleep(0.3)
    pub.publish("Final message")

    time.sleep(1)
    sub1.stop()
    sub2.stop()
    pub.close()
    print("Done.")


if __name__ == "__main__":
    demo()
