# p2000-sdr

P2000 is the ducth emergancy services messaging system for pagers. The system works on the 169,650 MHz FM band. These messages can be viewed with a FM receiver and the FLEX decoder. This script make yse of a sdr dongle and a FLEX decoder to show the messages.

## Requirements

    - RTL SDR Dongle (DBV-T, FM, DAB usb) based on a RTL chipset (820T2)
    - python 3
    - librtlsdr
    - multimon-ng

## Installation

The installation guide is for Linux on a Raspberry Pi 2 with RTL-SDR Blog v4.

- Install the required libraries:
```commandline
sudo apt-get -y install cmake build-essential libusb-1.0-0 libusb-1.0-0-dev libpulse-dev libx11-dev
```
- Clone the rtl-sdr repo
```commandline
git clone https://github.com/osmocom/rtl-sdr.git
```

- Install the rtl-sdr package.
```commandline
cd rtl-sdr/
mkdir build
cd build
cmake ../ -DINSTALL_UDEV_RULES=ON
make
sudo make install
sudo ldconfig
```

- Blacklist drivers  
Some people are reporting problems with conflicting drivers, so you might want to do this as a precaution.  
```commandline
sudo nano /etc/modprobe.d/blacklist.conf
```
Add the following lines:
```commandline
blacklist dvb_usb_rtl28xxu
blacklist rtl2832
blacklist rtl2830
```
Now reboot for the changes to take effect:
```commandline
sudo reboot -h 0
```
# Installing p2000-sdr
```commandline
git clone https://github.com/CaelynInc/p2000-sdr.git
cd p2000-sdr
```

# Running the script

```bash
cd p2000-sdr
./p2000-sdr.py
```
