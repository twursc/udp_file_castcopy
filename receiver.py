# -*- coding: utf-8 -*-
# UDP CastCopy Tool - Receiver

# -- Configuration ------------
PORT = 59975
PACKET_CHECKSUM = 0

# -- Import Libraries ---------
import os
import sys
import time
import ctypes
import socket
import random
import base64
import zlib
import platform
from threading import Thread

# -- Initialize Socket --------
network = '<broadcast>'
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
s.bind(('', PORT))
associated_caster = '<broadcast>'
Thread1_Running = False
local_client_id = ''

# -- Functions ----------------
def Terminate(code, reason):
	print('\n ! ' + reason)
	print(' - Exiting...')
	s.close()
	quit(code)

def castStr(string):
	s.sendto(string.encode('utf-8'), (network, PORT))

def ask_linkspeed():
	while True:
		sys.stdout.write('\r - Enter your NIC link speed (in unit Mbps): ')
		try:
			#link_speed = int(raw_input())
			link_speed = 100
			print('100')
		except:
			print(' ! The value is incorrect.')
		else: 
			break
	return link_speed

def formatBytes(bytes):
	rBytes = float(bytes)
	sqr = 1
	sqt = ['Bytes', 'KB', 'MB', 'GB', 'TB']
	fbs = ""
	while sqr <= 4:  # max 1024 TB supported
		if rBytes/pow(1024, sqr) < 1:
			fbv = "%.2f" % float(rBytes/(pow(1024, sqr-1)))
			fbs = fbv+" "+sqt[sqr-1]
			break
		else:
			sqr += 1
	return fbs

def targetdiskspace(path):
	if platform.system() == 'Windows':
		fpath = path.replace("\\", "/")
		fdrive = fpath.split("/")[0]
		free_bytes = ctypes.c_ulonglong(0)
		ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(fdrive), None, None, ctypes.pointer(free_bytes))
		return free_bytes.value
	else:
		st = os.statvfs(os.path.dirname(path))
		return st.f_bavail * st.f_frsize

class OnlineStatusReporter(Thread):
	def __init__(self):
		Thread.__init__(self)
	def run(self):
		while True:
			if Thread1_Running:
				time.sleep(3)
				try: castStr('CASTCOPY|CLIENT_SETONLINE|'+local_client_id)
				except:
					print(" - Socket closed.")
					break
			else:
				break



if __name__ == '__main__': 
	# -- Main thread --------------
	print('UDP Castcopy Client')

	# -- Ask link speed -----------
	link_speed = ask_linkspeed()
	save_dest = ""

	# -- Search caster ------------
	sys.stdout.write(' - Searching for caster... ')
	castStr('CASTCOPY|SEARCH_SERVER|1')
	while True:
		data, address = s.recvfrom(65535)
		#print data.decode('utf-8')
		if data.decode('utf-8')[:25] == "CASTCOPY|SEARCH_RESPONSE|":
			print("Found: " + address[0])
			response = data.decode('utf-8').split('|')
			save_dest = response[2]
			space_available = targetdiskspace(save_dest)
			# check if target exists
			try: os.path.getsize(save_dest)
			except: pass
			else: 
				Terminate(1, "Target file exists, please remove it first.")
			# check target space sufficiency
			if space_available == 0:
				Terminate(1, 'Did you specified a wrong path at the server? The target path is inaccessible.')
			if space_available <= long(response[3]):
				print(' ! Target location "'+save_dest+'" has no enough space, ')
				Terminate(1, formatBytes(long(response[3]))+' required, '+formatBytes(targetdiskspace(save_dest))+' available.')
			break

	# -- handshake with caster ----
	print(' - Announcing new client...')
	while True:
		gen_client_id = hex(random.randint(4096, 65536))[2:].upper()
		online_announcement_str = 'CASTCOPY|CLIENT_ANNOUNCEMENT|' + gen_client_id
		castStr(online_announcement_str)

		data, address = s.recvfrom(65535)
		if data.decode('utf-8') == online_announcement_str:
			data, address = s.recvfrom(65535)

		#print '\"'+data.decode('utf-8')+'\"'
		if data.decode('utf-8') == "CASTCOPY|CLIENTREG_ACCEPTED|" + gen_client_id:
			# associated with caster
			print ' * Client accepted to the broadcast session.'
			print ' * Associated caster: '+address[0]+', Client ID: '+gen_client_id
			associated_caster = address[0]
			local_client_id = gen_client_id
			break
		else:
			print ' * Server rejected, retrying...'
			time.sleep(2)


	# -- wait for broadcast and 
	#    send keep-alive packet ---
	try:
		thread_1 = OnlineStatusReporter()
		Thread1_Running = True
		thread_1.start()
	except:
		Terminate(1, 'Threading failed.')

	print ' - Waiting for broadcast... '

	fh = open(save_dest, 'wb')
	s.settimeout(0.1)
	time_startCast = 0
	time_stopCast = 0
	while True:
		try: data, address = s.recvfrom(65535)
		except: pass
		else:
			if data.decode('utf-8') == "CASTCOPY|WAIT_READY|0":
				print(" - Sending READY signal to "+address[0]+"...")
				castStr("CASTCOPY|CLIENT_READY|" + local_client_id)
				Thread1_Running = False
			if address[0] == associated_caster and data.decode('utf-8')[:17] == "CASTCOPY|CASTSEG|":
				if time_startCast == 0: time_startCast = time.time()
				response = data.decode('utf-8').split("|")
				segment = response[2]
				crchash = int(response[3])
				b64data = response[4]
				# checksum
			#	if zlib.crc32(b64data) == crchash:
			#	print(" - Saved segment " + segment)
				fh.write(base64.b64decode(b64data))
				sys.stdout.write("\r - Sending ACK_" + segment + " to " + address[0])
				castStr("CASTCOPY|CASTACK|" + local_client_id + "|" + segment)
			#	else:
			#		print(" ! Checksum failed, NAKed segment " + segment)				
			#		castStr("CASTCOPY|CASTNAK|" + local_client_id + "|" + segment)
			if address[0] == associated_caster and data.decode('utf-8')[:21] == "CASTCOPY|CASTFINISHED":
				time_stopCast = time.time()
				print("\n - Receive finished.")
				print(" - Time elapsed: " + str(int(time_stopCast - time_startCast)) + " s")
				s.close()
				fh.close()
				break

	raw_input()
