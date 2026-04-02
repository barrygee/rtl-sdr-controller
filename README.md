# RTL-SDR TCP Server — Docker Setup

Runs `rtl_tcp` in a Docker container on a Raspberry Pi (or any Linux host), accessible from any machine on the network.

---

## Files

```
Dockerfile          — builds rtl_tcp from source
docker-compose.yml  — runs the container with USB passthrough
```

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
scp Dockerfile docker-compose.yml pi@<PI_IP>:~/rtl-sdr/
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

Expected output:
```
Found 1 device(s):
  0:  Realtek, RTL2838UHIDIR, SN: ...
Using device 0: Generic RTL2832U OEM
...
listening on port 1234
```

---

## Step 5 — Connect from another machine

Use any RTL-TCP compatible client (SDR#, GQRX, SDR++, etc.):

```
Host: <PI_IP>
Port: 1234
```

**GQRX:** Configure I/O Devices → RTL-SDR TCP → set host and port

**SDR#:** Source → RTL-SDR (TCP) → set host and port

**SDR++:** Source → RTL-SDR TCP → set host and port

---

## Useful commands

```bash
docker compose down       # stop
docker compose up -d      # start (no rebuild)
docker compose restart    # restart
hostname -I               # find Pi's IP address
```

---

## Notes

- Port `1234` is the standard `rtl_tcp` port
- `privileged: true` is required for USB passthrough
- Plug the dongle in before starting the container
- The blacklist step must be done on the Pi host, not inside Docker
