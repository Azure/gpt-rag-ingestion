# **XG-9000 WiFi Modem Technical Manual**

## **Table of Contents**

1. Introduction  
   1.1. Overview  
   1.2. Package Contents  
   1.3. Safety Precautions  
2. Hardware Specifications  
   2.1. General Specifications  
   2.2. Ports and Interfaces  
   2.3. LED Indicators  
   2.4. Power Supply  
3. Installation and Setup  
   3.1. Unboxing the Modem  
   3.2. Connecting the Modem  
   3.3. Initial Power-Up  
   3.4. Accessing the Web Interface  
   3.5. Quick Setup Wizard  
4. Advanced Configuration  
   4.1. WiFi Settings  
   4.2. Security Features  
   4.3. LAN Configuration  
   4.4. Firewall and Parental Controls  
   4.5. QoS Settings  
   4.6. VPN Setup  
5. Troubleshooting  
   5.1. Common Issues and Solutions  
   5.2. Resetting the Modem  
   5.3. Firmware Update  
   5.4. Contacting Support  
6. Glossary of Terms  
7. Technical Support and Warranty  

---

## **1. Introduction**

### **1.1. Overview**

Congratulations on your purchase of the XG-9000 WiFi Modem, a cutting-edge device designed to deliver exceptional internet connectivity with unparalleled speed and reliability. The XG-9000 integrates advanced technology, making it the perfect choice for homes and small businesses. With dual-band connectivity, a high-performance antenna array, and a robust feature set, this modem ensures that your network remains secure and efficient.

This manual provides comprehensive guidance on setting up and managing your XG-9000 WiFi Modem. Whether you are a novice user or an experienced technician, the step-by-step instructions, detailed diagrams, and troubleshooting tips included in this manual will help you maximize the performance and functionality of your device.

### **1.2. Package Contents**

Your XG-9000 WiFi Modem package includes the following items:

- XG-9000 WiFi Modem
- Power Adapter (12V, 2A)
- Ethernet Cable (Cat 6, 1.5 meters)
- Quick Start Guide
- Warranty Card
- Wall Mounting Kit (Optional)

Please verify that all items are present before proceeding with the installation. If any components are missing, contact your retailer or the manufacturer for assistance.

### **1.3. Safety Precautions**

For your safety and the longevity of the device, please observe the following precautions:

- **Avoid Water Exposure:** Do not expose the modem to water or moisture. Keep it in a dry, well-ventilated area.
- **Power Requirements:** Use only the supplied power adapter. Using an incompatible adapter may cause damage to the modem or create a safety hazard.
- **Heat Exposure:** Keep the modem away from direct sunlight and other heat sources to prevent overheating.
- **Cleaning:** Clean the modem using a soft, dry cloth. Do not use chemical cleaners or solvents.
- **Handling:** Handle the modem with care, avoiding excessive force or dropping the device.

---

## **2. Hardware Specifications**

### **2.1. General Specifications**

The XG-9000 WiFi Modem is engineered with state-of-the-art components to ensure superior performance and durability. Below are the key specifications:

- **Processor:** Quad-core ARM Cortex-A55 1.5 GHz
- **Memory:** 512MB DDR4 RAM
- **Flash Storage:** 256MB NAND Flash
- **Wireless Standards:** IEEE 802.11a/b/g/n/ac/ax
- **Frequencies:** 2.4 GHz and 5 GHz dual-band
- **Maximum Data Rate:** 6 Gbps (combined)
- **Antennas:** 8x High-Gain Antennas (4x Internal, 4x External)
- **Dimensions:** 240mm x 160mm x 30mm
- **Weight:** 500g

### **2.2. Ports and Interfaces**

The modem comes equipped with multiple ports to cater to a variety of network setups:

- **WAN Port:** 1x Gigabit Ethernet (RJ-45)
- **LAN Ports:** 4x Gigabit Ethernet (RJ-45)
- **USB Ports:** 2x USB 3.0
- **Power Input:** 12V DC jack
- **Reset Button:** Factory reset functionality
- **WPS Button:** Easy WiFi setup

### **2.3. LED Indicators**

The XG-9000 features a series of LED indicators on the front panel to provide real-time status updates:

- **Power LED:** Solid green indicates power on; flashing green indicates startup.
- **WAN LED:** Solid blue indicates an active internet connection; red indicates no connection.
- **LAN LEDs:** Solid green indicates active Ethernet connections; flashing green indicates data transfer.
- **WiFi LEDs:** Solid blue indicates WiFi is enabled; flashing blue indicates data transmission.
- **USB LEDs:** Solid yellow indicates connected devices; flashing yellow indicates active data transfer.

### **2.4. Power Supply**

The XG-9000 is powered by a 12V, 2A DC adapter. It is recommended to use only the provided power adapter to ensure optimal performance and avoid damage to the modem.

---

## **3. Installation and Setup**

### **3.1. Unboxing the Modem**

Carefully remove the XG-9000 from its packaging. Ensure that all accessories are present and in good condition. If the modem appears damaged or any accessories are missing, contact your retailer immediately.

### **3.2. Connecting the Modem**

1. **WAN Connection:** Connect the modem to your internet source (e.g., DSL, fiber, or cable modem) using the included Ethernet cable. Plug one end of the cable into the WAN port on the XG-9000 and the other end into your internet source's Ethernet port.
  
2. **LAN Connection:** If you wish to connect devices directly to the modem via Ethernet, use additional Ethernet cables to connect them to the LAN ports.
  
3. **Power Connection:** Plug the power adapter into the modem's power input and the other end into a wall outlet. Press the power button to turn on the modem.

### **3.3. Initial Power-Up**

Once powered on, the modem will go through a startup sequence, indicated by the flashing power LED. This process may take up to two minutes. Once complete, the LED will turn solid green, indicating the modem is ready for use.

### **3.4. Accessing the Web Interface**

To configure the modem, access its web interface:

1. **Connect to the Modem:** Using a computer or mobile device, connect to the modem’s default WiFi network. The network name (SSID) and password are printed on a label on the bottom of the modem.
   
2. **Open a Web Browser:** Enter `http://192.168.0.1` in the address bar. You will be prompted to enter a username and password. The default username is `admin`, and the default password is `password`.
   
3. **Login:** Upon successful login, you will be redirected to the modem's dashboard.

### **3.5. Quick Setup Wizard**

The Quick Setup Wizard will guide you through the initial configuration:

1. **Internet Connection:** Select your connection type (e.g., DHCP, PPPoE) and enter any required information, such as username and password for PPPoE.
  
2. **WiFi Settings:** Customize your WiFi network names (SSID) and passwords for both the 2.4 GHz and 5 GHz bands. Enable or disable guest networks as needed.
  
3. **Save Settings:** Once all settings are configured, click “Save” to apply the changes. The modem will reboot to finalize the setup process.

---

## **4. Advanced Configuration**

### **4.1. WiFi Settings**

The XG-9000 offers extensive WiFi customization options:

- **SSID Broadcast:** Enable or disable the broadcast of your network name to make your network visible or hidden.
- **Channel Selection:** Manually select a WiFi channel or allow the modem to choose the optimal channel automatically.
- **Bandwidth Control:** Limit the bandwidth available to connected devices to ensure a balanced network load.
- **WiFi Scheduler:** Set specific times when WiFi should be enabled or disabled.

### **4.2. Security Features**

Security is a top priority with the XG-9000, which includes the following security features:

- **WPA3 Encryption:** Protect your network with the latest WiFi security standard.
- **MAC Filtering:** Allow or deny access to devices based on their MAC addresses.
- **Firewall Settings:** Enable or disable the firewall, configure port forwarding, and set up DMZ for specific devices.
- **Parental Controls:** Restrict access to specific websites or services for designated devices.

### **4.3. LAN Configuration**

Manage your local area network (LAN) settings with these options:

- **IP Address Management:** Configure the modem's IP address and subnet mask.
- **DHCP Server:** Enable or disable the built-in DHCP server and customize the IP address range.
- **Static IP Assignment:** Assign fixed IP addresses to specific devices on your network.

### **4.4. Firewall and Parental Controls**

The XG-9000 includes robust firewall features to protect your network from unauthorized access and malicious threats. Parental controls allow you to manage and restrict internet usage for specific devices:

- **Firewall Settings:** The firewall can be

 enabled or disabled from the settings menu. Customize firewall rules, including blocking specific ports, IP addresses, or protocols to enhance security. You can also configure the modem's DMZ (Demilitarized Zone) settings for devices that require unrestricted internet access, such as gaming consoles.
  
- **Parental Controls:** The parental controls feature allows you to set time-based access restrictions for specific devices, ensuring a balanced and controlled internet usage environment. You can block access to particular websites or categories, set time limits for internet usage, and monitor activity. All these settings can be managed through the web interface or the XG-9000 mobile app.

### **4.5. QoS Settings**

Quality of Service (QoS) settings prioritize network traffic to ensure optimal performance for critical applications:

- **Bandwidth Allocation:** Allocate a specific percentage of your total bandwidth to devices or applications that require high-speed connectivity.
  
- **Application Prioritization:** Prioritize certain applications like video streaming or online gaming to reduce latency and buffering.
  
- **Device Prioritization:** Assign priority to specific devices on your network, ensuring they receive the bandwidth they need for smooth performance.

### **4.6. VPN Setup**

The XG-9000 supports Virtual Private Network (VPN) configurations, allowing secure remote access to your home network:

- **PPTP/L2TP/IPSec:** Configure the modem to act as a VPN server or client, supporting various VPN protocols for secure connections.
  
- **Remote Access:** Enable remote management of the modem via VPN, allowing you to configure settings from anywhere with an internet connection.
  
- **Split Tunneling:** Configure split tunneling to route only specific traffic through the VPN while allowing other traffic to access the internet directly.

---

## **5. Troubleshooting**

### **5.1. Common Issues and Solutions**

#### **No Internet Connection**
- **Issue:** The WAN LED is red, and there is no internet connection.
- **Solution:** Check that the Ethernet cable is securely connected to both the modem and your internet source. Reboot both the modem and the internet source. If the issue persists, contact your ISP.

#### **Cannot Access the Web Interface**
- **Issue:** Unable to access `http://192.168.0.1`.
- **Solution:** Ensure your device is connected to the modem's network. Verify that the IP address is correct. If necessary, reset the modem to factory settings.

#### **Slow WiFi Speeds**
- **Issue:** WiFi speeds are slower than expected.
- **Solution:** Check for interference from other wireless devices or networks. Change the WiFi channel or move the modem to a more central location. Ensure that the modem's firmware is up to date.

### **5.2. Resetting the Modem**

If you encounter persistent issues, you may need to reset the modem to its factory settings:

1. **Locate the Reset Button:** The reset button is typically found on the back panel of the modem.
  
2. **Press and Hold:** Using a paperclip or similar object, press and hold the reset button for 10 seconds.
  
3. **Reboot:** The modem will reboot, and all settings will be restored to their factory defaults. Reconfigure the modem using the Quick Setup Wizard.

### **5.3. Firmware Update**

Keeping the modem’s firmware up to date ensures you have the latest features and security patches:

1. **Check for Updates:** Access the web interface and navigate to the firmware update section.
  
2. **Download:** If an update is available, download it directly from the manufacturer’s website.
  
3. **Install:** Follow the on-screen instructions to install the firmware update. The modem will reboot automatically after the update is complete.

### **5.4. Contacting Support**

If you require further assistance, contact our technical support team:

- **Phone:** 1-800-555-1234
- **Email:** support@modemco.com
- **Website:** www.modemco.com/support

Please have your modem’s serial number and a detailed description of the issue ready when contacting support.

---

## **6. Glossary of Terms**

- **SSID:** Service Set Identifier, the name of a WiFi network.
- **WAN:** Wide Area Network, typically refers to your internet connection.
- **LAN:** Local Area Network, refers to the internal network within your home or business.
- **IP Address:** A unique address assigned to each device on a network.
- **DHCP:** Dynamic Host Configuration Protocol, a network protocol that automatically assigns IP addresses to devices.
- **MAC Address:** Media Access Control address, a unique identifier assigned to network interfaces.
- **WPA3:** WiFi Protected Access 3, the latest WiFi security protocol.

---

## **7. Technical Support and Warranty**

The XG-9000 WiFi Modem comes with a 2-year limited warranty, covering manufacturing defects and hardware malfunctions. If your modem fails within this period, you may be eligible for a repair or replacement. Please retain your proof of purchase and refer to our warranty policy for more details.

For extended support, we offer a range of premium services, including remote setup assistance, network optimization, and priority technical support. Visit our website or contact customer service to learn more.
