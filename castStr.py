import sys
import socket

PORT = 59975
network = '<broadcast>'
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

while True:
	sys.stdout.write("Broadcast: ")
	s.sendto(raw_input().encode("utf-8"), (network, PORT))