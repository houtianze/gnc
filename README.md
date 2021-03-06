## gnc (Go NetCat)

---
**Mod**

Add `-X connect` and `-x proxy:port` switches to make it a drop-in replacement for netcat for OpenSSH `ProxyCommand` like this:
```
ProxyCommand gnc -X connect -x proxy:8080 %h %p
```
---


The Go-netcat is simple implementation of the netcat utility in go that allows to listen and send data over TCP and UDP protocols.

This utility was created for Golang learning purposes and was inspired by https://github.com/dddpaul/go-netcat

### Usage:

```
gnc [-lu] [-p source port ] [-s source ip address ] [hostname ] [port[s]]
```

### Description:

The utility allows to listen UDP\TCP ports and send data to remote ports over TCP\UDP. Main usage scenario is testing network protocols and accessibility of the open ports.

The options are as follows:

**-l**
	Used to specify that go-nc should listen for an incoming connection rather than initiate a connection to a remote host.

**-p** __port__
	Specifies the source port netcat should use, subject to privilege restrictions and availability.

**-u**
	Use UDP instead of the default option of TCP.

### Examples:

**$ gnc hostname 42**

Open a TCP connection to port 42 of hostname.

**$ gnc -u hostname 53**

Open a UDP connection to port 53 of hostname.

**$ gnc -l 3000**

Listen on TCP port 3000, and once there is a connection, send stdin to the remote host, and send data from the remote host to stdout.

**$ gnc -u -l 3000**

Listen on UDP port 3000, and once there is a connection, send stdin to the remote host, and send data from the remote host to stdout.
