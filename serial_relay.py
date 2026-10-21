#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CHUNITHM 手台串口中继（协议网关）

和 serial_proxy.py 的区别：
  * serial_proxy 只是原样转发，游戏收到什么就是什么（Wine 分帧怎么给就怎么给）；
  * 本脚本自己驱动手台协议，对设备保持 200Hz 轮询，对游戏**严格按协议应答**：
       游戏发 0xB0  -> 立刻回 1 字节 0xB0
       游戏发 0xB1  -> 立刻回最新的 33 字节输入快照
       其他（LED 帧）-> 原样转发给设备
    这样游戏永远拿到完整、成帧、且最新的数据，不会再因为半包/错位而走进
    chuniio 里那两处 Sleep(1000) 重试路径。

用法：
  python3 tools/serial_relay.py            # 自动找设备，占用 com3
  python3 tools/serial_relay.py --com 3 --hz 200
  # 另开终端正常启动游戏即可；Ctrl-C 退出并还原 com3

只读游戏侧：不会修改游戏文件，退出自动还原 COM 映射。
"""

import argparse
import collections
import glob
import os
import pty
import select
import signal
import struct
import subprocess
import sys
import termios
import threading
import time
import tty

DOSDEV = "/mnt/ZhiTai/chunithm/game/pfx/dosdevices"
CMD_PING = 0xB0
CMD_INPUT = 0xB1
REPLY_LEN = 33


def inj_bit(slot):
    """确定性的伪随机 0/1 序列：对称方波在特定延迟下会自相抵消，随机序列不会。"""
    return ((slot * 2654435761) >> 16) & 1


def resolve_device():
    for p in sorted(glob.glob("/dev/serial/by-id/*Nyanithm*")):
        try:
            if os.path.realpath(p).startswith("/dev/ttyACM"):
                return p
        except OSError:
            pass
    cands = sorted(glob.glob("/dev/ttyACM*"))
    return cands[-1] if cands else None


def open_raw(path, baud=115200):
    fd = os.open(path, os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK)
    tty.setraw(fd)
    a = termios.tcgetattr(fd)
    sp = getattr(termios, f"B{baud}", termios.B115200)
    a[4] = a[5] = sp
    termios.tcsetattr(fd, termios.TCSANOW, a)
    return fd


class Relay:
    def __init__(self, device, com, hz, verbose=False, dosdev=None):
        self.dosdev = dosdev or DOSDEV
        self.device_path = device
        self.com = com
        self.period = 1.0 / hz
        self.verbose = verbose

        self.master = None
        self.slave = None
        self.slave_name = None
        self.dev = None

        self.snapshot = None          # 最新 33 字节
        self.snapshot_t = 0.0
        self.dev_lat = []             # 设备往返
        self.dev_fail = 0
        self.req_count = 0            # 游戏请求数
        self.led_bytes = 0
        self.opened_by = []
        self.saved_link = None
        self.monitor = False
        self.last_act = None
        self.last_vec = None
        self.changes = 0
        self.last_peak = 0
        self.last_change_print = 0.0
        self.trace_cell = 0
        self.trace_val = None
        self.trace_t = 0.0
        self.lock = threading.Lock()
        self.led_buf = bytearray()
        self.outq = collections.deque()   # 待写给游戏的完整帧
        self.dropped = 0
        self.gain = 1.0
        self.snap = 0
        self.lead = 0.0
        self.hyst = 0
        self.on_state = bytearray(32)
        self.prev_snap = None
        self.prev_t = 0.0
        self.beep = None
        self.peak = 0
        self.inj_cell = 0
        self.inj_hz = 0.5
        self.inj_phase = None

    # ------------------------------------------------------------ 准备
    def setup(self):
        self.master, self.slave = pty.openpty()
        self.slave_name = os.ttyname(self.slave)
        tty.setraw(self.slave)
        os.set_blocking(self.master, False)

        path = os.path.join(self.dosdev, f"com{self.com}")
        if os.path.lexists(path):
            os.rename(path, path + ".relaybak")
            self.saved_link = os.readlink(path + ".relaybak") if os.path.islink(path + ".relaybak") else "file"
        os.symlink(self.slave_name, path)
        print(f"com{self.com}: {self.saved_link}  ->  {self.slave_name}")

        self.dev = open_raw(self.device_path)
        print(f"设备: {self.device_path} -> {os.path.realpath(self.device_path)}")

    # ------------------------------------------------------------ 设备侧
    def drain(self, fd, limit=4096):
        out = b""
        try:
            while len(out) < limit:
                d = os.read(fd, 512)
                if not d:
                    break
                out += d
        except (BlockingIOError, OSError):
            pass
        return out

    def poll_device(self):
        """发一次 0xB1，凑满 33 字节。返回是否成功。"""
        with self.lock:
            if self.led_buf:
                try:
                    os.write(self.dev, bytes(self.led_buf))
                except OSError:
                    pass
                self.led_buf.clear()
        self.drain(self.dev)                    # 丢掉残留，保证从干净边界开始
        t0 = time.monotonic()
        try:
            os.write(self.dev, bytes([CMD_INPUT]))
        except OSError:
            return False
        buf = b""
        deadline = t0 + 0.012
        while len(buf) < REPLY_LEN and time.monotonic() < deadline:
            r, _, _ = select.select([self.dev], [], [], 0.004)
            if not r:
                continue
            try:
                d = os.read(self.dev, 128)
            except (BlockingIOError, OSError):
                continue
            if d:
                buf += d
        if len(buf) >= REPLY_LEN:
            self.snapshot = buf[:REPLY_LEN]
            if self.trace_cell:
                v = self.snapshot[self.trace_cell - 1]
                if v != self.trace_val:
                    now_t = time.monotonic()
                    dt = (now_t - self.trace_t) * 1000 if self.trace_val is not None else 0.0
                    print(f"  (+{dt:6.1f}ms) 格子{self.trace_cell} = {v:3d}", flush=True)
                    self.trace_val = v
                    self.trace_t = now_t
            if self.monitor:
                vec = bytes(self.snapshot[:32])
                if self.last_vec is not None and vec != self.last_vec:
                    self.changes += 1
                    now_m = time.monotonic()
                    if now_m - self.last_change_print > 0.02:
                        self.last_change_print = now_m
                        diff = [i + 1 for i in range(32) if vec[i] != self.last_vec[i]]
                        print(f"  Δ [{time.strftime('%H:%M:%S')}.{int(time.time()*1000)%1000:03d}] "
                              f"格子{diff[:6]} -> {[vec[i-1] for i in diff[:6]]}", flush=True)
                self.last_vec = vec
                act = [i + 1 for i in range(32) if self.snapshot[i] > 40]
                air = self.snapshot[32]
                if self.snapshot:
                    self.peak = max(self.peak, max(self.snapshot[:32]))
                if not act:
                    self.peak = 0
                if act != self.last_act or air:
                    peak = max(self.snapshot[:32])
                    print(f"  [{time.strftime('%H:%M:%S')}] 手台侧: 按下格子 {act or '-'}  "
                          f"峰值 {peak:3d}  air {air:02x}")
                    self.last_act = act
            self.snapshot_t = time.monotonic()
            self.dev_lat.append((self.snapshot_t - t0) * 1000)
            if len(self.dev_lat) > 200:
                self.dev_lat.pop(0)
            return True
        self.dev_fail += 1
        return False

    # ------------------------------------------------------------ 游戏侧
    def reply(self, data):
        """按整帧排队发送：短写只保留剩余部分，绝不把一帧劈成两半。"""
        if not data:
            return
        self.outq.append(bytes(data))
        self.flush_out()

    def flush_out(self):
        while self.outq:
            head = self.outq[0]
            try:
                n = os.write(self.master, head)
            except BlockingIOError:
                break
            except OSError:
                self.outq.clear()
                return
            if n < len(head):
                self.outq[0] = head[n:]
                break
            self.outq.popleft()
        # 积压太多就丢最旧的整帧，保证游戏读到的永远是新数据、且帧对齐
        while len(self.outq) > 32:
            self.outq.popleft()
            self.dropped += 1

    def handle_game_write(self, data):
        """游戏的写分两类：1 字节的命令（b0/b1），和 96~205 字节的 LED 帧。
        按"整段长度"判断，比逐字节解析安全——LED 帧里本来就可能有 0xB0/0xB1。"""
        if len(data) <= 4 and all(b in (CMD_PING, CMD_INPUT) for b in data):
            for b in data:
                self.req_count += 1
                if b == CMD_PING:
                    self.reply(bytes([CMD_PING]))
                elif self.snapshot:
                    self.reply(self._prepare_reply(self.snapshot))
                else:
                    self.reply(bytes([CMD_PING]))   # 还没快照时先敷衍一下
            return
        with self.lock:
            self.led_buf.extend(data)
            if len(self.led_buf) > 8192:
                del self.led_buf[:-4096]
        self.led_bytes += len(data)
        if self.verbose:
            print(f"  LED {len(data)}B -> 设备")

    def _prepare_reply(self, snap):
        out = bytearray(snap)
        if self.gain != 1.0:
            # 只放大 32 个传感器值，第 33 字节是 air 位图，不能动
            for i in range(32):
                v = out[i] * self.gain
                out[i] = 255 if v > 255 else int(v)
        if self.snap:
            # 把渐变的模拟值在阈值处二值化成 0/255：游戏看到的是一条干净的阶跃沿
            now_t = time.monotonic()
            prev, dt = self.prev_snap, now_t - self.prev_t
            self.prev_snap, self.prev_t = bytes(snap), now_t
            for i in range(32):
                v = float(out[i])
                if self.lead > 0 and prev is not None and 0 < dt < 0.1:
                    # 按上升斜率外推 lead 毫秒：值正在爬升时提前触发
                    slope = (v - prev[i]) / (dt * 1000.0)   # 每毫秒变化
                    if slope > 0:
                        v = v + slope * self.lead
                # 迟滞：超过 snap 置位，跌到 snap-hyst 才复位，避免阈值附近抖动
                if v > self.snap:
                    self.on_state[i] = 1
                elif v < self.snap - self.hyst:
                    self.on_state[i] = 0
                out[i] = 255 if self.on_state[i] else 0
        if not self.inj_cell:
            return bytes(out)
        phase = inj_bit(int(time.monotonic() * self.inj_hz * 2))
        if phase:
            if self.inj_cell == -1:
                for i in range(32):
                    out[i] = 255
            else:
                out[self.inj_cell - 1] = 255
        if phase != self.inj_phase:
            self.inj_phase = phase
            print(f"  ★ [{time.strftime('%H:%M:%S')}] 注入 格子{self.inj_cell} -> "
                  + ("ON  (现在看游戏画面)" if phase else "OFF"), flush=True)
            if self.beep:
                try:
                    subprocess.Popen(["paplay", self.beep],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                except Exception:
                    pass
        return bytes(out)

    def _poller(self):
        """设备轮询跑在独立线程：它卡住也不会拖慢对游戏的应答。"""
        while not self._stop.is_set():
            t0 = time.monotonic()
            try:
                self.poll_device()
            except Exception as e:
                print(f"  轮询异常: {e}")
            rest = self.period - (time.monotonic() - t0)
            if rest > 0:
                time.sleep(rest)

    # ------------------------------------------------------------ 主循环
    def run(self):
        self._stop = threading.Event()
        threading.Thread(target=self._poller, daemon=True).start()
        print("Running...\n")
        last_stat = time.monotonic()
        last_req = 0
        while True:
            now = time.monotonic()
            if self.outq:
                self.flush_out()
            r, _, _ = select.select([self.master], [], [], 0.002)
            if r:
                try:
                    data = os.read(self.master, 512)
                except (BlockingIOError, OSError):
                    data = b""
                if data:
                    self.handle_game_write(data)

            if now - last_stat >= 1.0:
                rate = self.req_count - last_req
                last_req = self.req_count
                lat = sorted(self.dev_lat[-200:])
                lat_s = f"{lat[len(lat)//2]:.2f}ms" if lat else "-"
                age = (time.monotonic() - self.snapshot_t) * 1000 if self.snapshot else -1
                upd, self.changes = self.changes, 0
                print(f"  [{time.strftime('%H:%M:%S')}] 值更新 {upd:3d}/s  游戏请求 {rate:4d}/s  积压{len(self.outq):2d}/丢弃{self.dropped:<4d} "
                      f"设备往返 {lat_s}   快照 {age:6.1f}ms   失败 {self.dev_fail}")
                last_stat = now

    # ------------------------------------------------------------ 收尾
    def cleanup(self):
        try:
            self._stop.set()
        except Exception:
            pass
        path = os.path.join(self.dosdev, f"com{self.com}")
        try:
            if os.path.islink(path):
                os.unlink(path)
            if os.path.lexists(path + ".relaybak"):
                os.rename(path + ".relaybak", path)
                print(f"已还原 com{self.com}")
        except OSError as e:
            print(f"还原失败: {e}")
        for fd in (self.master, self.slave, self.dev):
            try:
                if fd is not None:
                    os.close(fd)
            except OSError:
                pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--device", default=None)
    ap.add_argument("--com", type=int, default=3)
    ap.add_argument("--hz", type=float, default=200.0, help="对设备的轮询频率")
    ap.add_argument("--dosdev", default=None)
    ap.add_argument("--monitor", action="store_true",
                    help="终端里实时显示手台侧的触摸状态，用来和游戏画面做左右对照")
    ap.add_argument("--gain", type=float, default=1.0,
                    help="放大传感器数值（只影响发给游戏的副本），用来验证游戏侧阈值是否偏高")
    ap.add_argument("--inject", default="",
                    help="注入测试，例：1@0.5 表示让 1 号格子以 0.5Hz 闪烁，用来量游戏画面跟不跟得上")
    ap.add_argument("--snap", type=int, default=0,
                    help="把传感器值在指定阈值处二值化成 0/255（例：--snap 20），用来消除渐变斜坡")
    ap.add_argument("--hyst", type=int, default=0,
                    help="迟滞量：超过 --snap 置位，跌到 snap-hyst 才复位（例：--snap 20 --hyst 10）")
    ap.add_argument("--lead", type=float, default=0.0,
                    help="按斜率提前触发的毫秒数（配合 --snap 使用），例：--snap 20 --lead 25")
    ap.add_argument("--trace", type=int, default=0,
                    help="持续打印某个格子数值的每次变化（带间隔），用来量触摸的上升时间。例：--trace 1")
    ap.add_argument("--beep", action="store_true", help="注入翻转时响一声，用来对照屏幕")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    dev = args.device or resolve_device()
    if not dev or not os.path.exists(dev):
        print("找不到手台，插好了吗？")
        return 1
    print(f"设备: {dev} ({os.path.realpath(dev)})")

    relay = Relay(dev, args.com, args.hz, args.verbose, args.dosdev)
    relay.monitor = args.monitor
    relay.trace_cell = args.trace
    relay.snap = args.snap
    relay.lead = args.lead
    relay.hyst = args.hyst
    if relay.snap:
        msg = f"二值化: 值 > {relay.snap} 的输出 255，否则 0"
        if relay.lead:
            msg += f"  (外加 {relay.lead:.0f}ms 斜率预测提前量)"
        if relay.hyst:
            msg += f"  [迟滞 {relay.hyst}]"
        print(msg)
    if args.inject:
        cell, _, hz = args.inject.partition("@")
        relay.inj_cell = int(cell)
        relay.inj_hz = float(hz) if hz else 0.5
        print(f"注入模式: 格子 {relay.inj_cell} 以 {relay.inj_hz}Hz 闪烁（ON 保持 {1/(2*relay.inj_hz):.2f}s）")
    if args.beep:
        import wave
        import numpy as np
        wav = "/tmp/chuni_click.wav"
        sr, n = 48000, 2400
        tt = np.arange(n) / sr
        tone = (0.6 * np.sin(2 * np.pi * 1500 * tt) * np.linspace(1, 0, n) * 32767).astype(np.int16)
        with wave.open(wav, "wb") as wf:
            wf.setnchannels(1); wf.setsampwidth(2); wf.setframerate(sr)
            wf.writeframes(tone.tobytes())
        relay.beep = wav
        print("提示音已启用")
    relay.gain = args.gain
    if relay.gain != 1.0:
        print(f"增益 x{relay.gain}: 发给游戏的传感器值已放大（air 字节不动）")
    try:
        relay.setup()
        relay.run()
    except KeyboardInterrupt:
        pass
    finally:
        print()
        print(f"游戏请求 {relay.req_count} 次, LED 转发 {relay.led_bytes} B, 设备失败 {relay.dev_fail} 次")
        if relay.dev_lat:
            lat = sorted(relay.dev_lat)
            print(f"设备往返 p50={lat[len(lat)//2]:.2f}ms max={lat[-1]:.2f}ms")
        relay.cleanup()
    return 0


if __name__ == "__main__":
    sys.exit(main())
