# RTL-SDR TCP Server — Docker Setup

Runs `rtl_tcp` in Docker on a Raspberry Pi (or any Linux host) behind a **fan-out relay**, so multiple clients can share one dongle and a client that disconnects abruptly never locks the dongle for the others. Accessible from any machine on the network.

---

## Files

```
Dockerfile          — builds rtl_tcp from source
docker-compose.yml  — runs rtl_tcp (loopback-only) + the relay
rtl_tcp_relay.py    — the fan-out relay (pure-stdlib Python, no dependencies)
```

---

## How it works

`rtl_tcp` serves exactly **one TCP client at a time** and sets no keepalive on it, so a client whose host sleeps, crashes, or drops off the network leaves `rtl_tcp` holding a dead connection — locking everyone else out until it's restarted.

To avoid that, the stack runs two services:

```
USB dongle → rtl-tcp (127.0.0.1:1235, loopback-only) → rtl-relay (0.0.0.0:1234) → your clients
```

- **`rtl-tcp`** binds **loopback-only** on an internal port (`1235`), so it's reachable only on the host, not the LAN.
- **`rtl-relay`** is the dongle's single permanent client. It re-serves the IQ stream to **any number of clients** on the public port (`1234`): replays the `rtl_tcp` magic header, fans out IQ, forwards tuning commands upstream (one physical tuner, so concurrent clients share tuning), and **reaps dead clients** via TCP keepalive. A watchdog restarts `rtl-tcp` (via the Docker socket) if the dongle ever wedges silently after a USB re-enumeration.

Clients still connect to the host on port **1234**, exactly as a plain `rtl_tcp` — the relay is transparent.

---

## Step 1 — Blacklist the DVB kernel driver (on the host machine)

The RTL-SDR dongle is often claimed by the Linux DVB kernel modules before `rtl_tcp` can access it. Blacklist them:

```bash
echo -e "blacklist dvb_usb_rtl28xxu\nblacklist rtl2832\nblacklist rtl2830" | sudo tee /etc/modprobe.d/rtl-sdr-blacklist.conf
```

Unload the modules if they're already loaded:

```bash
sudo rmmod dvb_usb_rtl28xxu 2>/dev/null
sudo rmmod rtl2832 2>/dev/null
sudo rmmod rtl2830 2>/dev/null
```

Persist the blacklist across reboots:

```bash
sudo update-initramfs -u
```

Increase USB memory limit (prevents buffer errors with high sample rates):

```bash
echo 0 | sudo tee /sys/module/usbcore/parameters/usbfs_memory_mb
```

To make this persist across reboots, add it to `/etc/rc.local` before the `exit 0` line:

```bash
echo 'echo 0 | tee /sys/module/usbcore/parameters/usbfs_memory_mb' | sudo tee -a /etc/rc.local
```

Reboot:

```bash
sudo reboot
```

---

## Step 2 — Install Docker (if not already installed)

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker
sudo systemctl enable docker
```

---

## Step 3 — Copy files to the Pi (if deploying remotely)

From your local machine:

```bash
scp Dockerfile docker-compose.yml rtl_tcp_relay.py pi@<PI_IP>:~/rtl-sdr/
ssh pi@<PI_IP>
cd ~/rtl-sdr
```

---

## Step 4 — Build and run

```bash
docker compose up -d --build
```

Check logs:

```bash
docker compose logs -f
```

Expected output — `rtl-tcp` finds the dongle, and `rtl-relay` reports it connected upstream and is listening:
```
rtl-tcp   | Found 1 device(s):
rtl-tcp   |   0:  Realtek, RTL2838UHIDIR, SN: ...
rtl-tcp   | Using device 0: Generic RTL2832U OEM
rtl-relay | Relay listening on 0.0.0.0:1234, fanning out 127.0.0.1:1235
rtl-relay | Upstream rtl_tcp connected (127.0.0.1:1235)
```

---

## Step 5 — Connect from another machine

Use any RTL-TCP compatible client (SDR#, GQRX, SDR++, etc.) — point it at the relay:

```
Host: <PI_IP>
Port: 1234
```

**GQRX:** Configure I/O Devices → RTL-SDR TCP → set host and port

**SDR#:** Source → RTL-SDR (TCP) → set host and port

**SDR++:** Source → RTL-SDR TCP → set host and port

Multiple clients can connect at once; they share the dongle's tuning.

---

## Useful commands

```bash
docker compose down               # stop
docker compose up -d              # start (no rebuild)
docker compose restart            # restart both services
docker compose logs -f rtl-relay  # follow the relay (client connect/disconnect, upstream status)
hostname -I                       # find the host's IP address
```

---

## Notes

- Port **`1234`** is the public port clients connect to — served by the relay.
- Port **`1235`** is the internal, loopback-only `rtl_tcp` port behind the relay (not exposed to the LAN).
- The relay mounts `/var/run/docker.sock` so its watchdog can restart `rtl-tcp` on a silent-dongle wedge. This grants the relay container root-equivalent host control — fine on a single-purpose device; remove the mount and `RELAY_RESTART_CONTAINER` env in `docker-compose.yml` to disable the watchdog.
- `privileged: true` + the USB-bus device passthrough are required so a re-enumerated dongle stays usable.
- Plug the dongle in before starting the container.
- The blacklist step must be done on the Pi host, not inside Docker.
