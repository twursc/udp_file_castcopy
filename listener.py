import socket

PORT = 59975
network = '<broadcast>'
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
s.bind(('', PORT))

print('Listening UDP broadcast at port '+str(PORT))

while True: 
	data, addr = s.recvfrom(65535)
	print('{0: >18}'.format(addr[0] + ": ") + data.decode('utf-8'))
