# -*- coding: utf-8 -*-
# UDP CastCopy Tool - Sender

# -- Configuration ------------
PORT = 59975
file_to_send = 'D:/Downloads/TDDownload/MK60.1-kltechnduo-201608210906-NIGHTLY.zip'
client_save_path = 'D:/TEST.ZIP'
rb_size = 32768

# -- Import Libraries ---------
import os
import sys
import socket
import time
import base64
import zlib
from threading import Thread

# -- Initialize Socket --------
network = '<broadcast>'
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
s.settimeout(1)
s.bind(('', PORT))

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


# -- Threads ------------------
class inactive_clients_cleaner(Thread):
	def __init__(self): Thread.__init__(self)
	def run(self):
		while True:
			if Thread2_Running:
				time.sleep(5)
				print("Thread-2: Running")
				for client_id in clients_list:
					if int(time.time()) - clients_list[client_id]["last_alive"] >= 5 and clients_list[client_id]["dead_client"] == False:
						print(" * Set inactive client [" + client_id + "] to dead state.")
						clients_list[client_id]["dead_client"] = True
						# print clients_list
			else:
				break

class wait_for_clients(Thread):
	def __init__(self): Thread.__init__(self)
	def run(self):
		print('\n - Waiting for clients...')
		print(' - Press [ENTER] if your all clients are currently registered.\n')
		while True:
			if Thread1_Running:
				data, address = s.recvfrom(65535)
				decoded_data = data.decode('utf-8')
				if decoded_data[:22] == 'CASTCOPY|SEARCH_SERVER':
					castStr("CASTCOPY|SEARCH_RESPONSE|"+client_save_path+"|"+str(file_size))
				if decoded_data[:28] == 'CASTCOPY|CLIENT_ANNOUNCEMENT':
					request_id = str(decoded_data[29:33])
					sys.stdout.write(' - Detected client ['+request_id+'] from {}\r'.format(address[0]))
					# check if the ID is occupied
					try: 
						clients_list[request_id] 
					except:
						castStr("CASTCOPY|CLIENTREG_ACCEPTED|"+request_id)
						print(' * Accepted client ['+request_id+'] from {}'.format(address[0]))
						clients_list[request_id] = {"last_ack_segment": 0, "client_addr": address[0], "last_alive": int(time.time()), "dead_client": False, "acked_segments": ""}
						# print(clients_list)
					else:
						castStr("CASTCOPY|CLIENTREG_REJECTED|"+request_id)
						print(' * Rejected client ['+request_id+'] from {}'.format(address[0]))
						# print(clients_list)
				if decoded_data[:25] == 'CASTCOPY|CLIENT_SETONLINE':
					request_id = str(decoded_data[26:30])
					try:
						clients_list[request_id] 
					except: 
						#castStr("CASTCOPY|CLIENT_SETONLINE_FAILED|NOT_REGISTERED|"+request_id)
						pass
					else:
						clients_list[request_id]['last_alive'] = int(time.time())
						#print(' # updated last_alive of '+request_id+' to '+str(int(time.time())))

# -- Initialize Threads -------
Thread1_Running = False
Thread2_Running = False

thread_1 = wait_for_clients()
thread_2 = inactive_clients_cleaner()
thread_2.start()

if __name__ == '__main__': 
	# -- Main thread --------------
	print('UDP Castcopy Server')
	clients_list = {}
	file_size = 0

	# -- Get file size ------------
	print(' - Getting target file information...')
	try: 
		file_size = os.path.getsize(file_to_send)
	except: 
		Terminate(1, 'Failed to get file size of "'+file_to_send+'"')

	# -- Ask link speed -----------
	link_speed = ask_linkspeed()

	# -- Search other casters -----
	print(' * NOTICE: Only one caster allowed to running at this pre-cast state ')
	print('       in this VLAN at the same time, and all receivers should start ')
	print('       after this caster entered WAITING CLIENTS state. ')
	print(' * Press [ENTER] to continue.')
	raw_input()

	sys.stdout.write(' - Searching for other server also at pre-cast state on the network...')
	castStr('CASTCOPY|SEARCH_SERVER|MODE_DETECT_EXCLUSION')
	data, address = s.recvfrom(65535)

	try:
		data, address = s.recvfrom(65535)
		# print('"'+data.decode('utf-8')[:25]+'"')
	except:
		print(' OK ')
		pass
	else:
		if data.decode('utf-8')[:25] == 'CASTCOPY|SEARCH_RESPONSE|': 
			Terminate(1, '\nOnly one server process at pre-cast state allowed at the same time.')
		else:
			print(' Obstructed ')

	# -- Wait for clients ---------
	s.settimeout(65535)
	try: 
		Thread1_Running = True
		thread_1.start()
		Thread2_Running = True
		raw_input()
		Thread1_Running = False
	except:
		Terminate(1, 'Failed to start a thread for waiting clients.')

	# -- Confirm broadcast info ---
	print(' - CastCopy Job Details:')
	print(' -    File Path: ' + file_to_send )
	print(' -         Size: ' + formatBytes(file_size) + ' (' + str(file_size) + ' Bytes)')
	print(' -   Link Speed: ' + str(link_speed) + ' Mbps')
	print('\n - Receivers List: ')
	alive_count = 0
	for rcvr in clients_list:
		if clients_list[rcvr]["dead_client"] == False:
			print(' -    ' + '{0: <22}'.format(rcvr + ' (' + clients_list[rcvr]['client_addr'] + ')') + 
				  '      Last alive: ' + str(clients_list[rcvr]['last_alive']))
			alive_count += 1 
	print(' -    ' + str(len(clients_list)) + ' receivers, ' + str(alive_count) + ' alive.')
	print('\n - The receiver application should be active until broadcast complete, once the receiver was kicked due to timeout waiting ACK, it won\'t able to join the broadcast session again.')
	print(' - Press [ENTER] to start broadcasting process.')
	raw_input()

	# -- Wait for receivers -------
	s.settimeout(2)
	sys.stdout.write(' - Synchronizing receivers... ')
	Thread2_Running = False
	castStr('CASTCOPY|WAIT_READY|0')
	while True:
		try: data, address = s.recvfrom(65535)
		finally:
			if data.decode('utf-8')[:22] == "CASTCOPY|CLIENT_READY|":
				request_id = data.decode('utf-8').split("|")[2]
				try:
					clients_list[request_id]["acked_segments"]
				except:  # Not my receiver, pass.
					pass
				else:
					clients_list[request_id]["acked_segments"] = 0
			# Review clients list
			clients_all_ready = True
			for rcvr in clients_list:
				if clients_list[rcvr]["dead_client"] == False and clients_list[rcvr]["acked_segments"] != 0:
					clients_all_ready = False
			if clients_all_ready: 
				print("OK")
				break


	# -- Stop SETONLINE monitor
	#    then start casting -------
	fh = open(file_to_send, 'rb')
	data = ''
	segment = 0
	b64data = ""
	crchash = 0
	last_packet_done = ""
	s.settimeout(0.5)
	time_startCast = time.time()
	time_stopCast = 0
	sys.stdout.write(' - Broadcasting SEGMENT ' + '{0: >16}'.format(str(segment)))

	while True:
		# loop through the file and cast it out
		if last_packet_done != False:
			data = fh.read(rb_size)
			segment += 1
			b64data = base64.b64encode(data)
			crchash = zlib.crc32(b64data)
		if len(data) > 0: # there're still some data can cast.
			last_packet_done == False
			sys.stdout.write('\r - Broadcasting SEGMENT ' + '{0: >32}'.format(str(segment)))
			castStr("CASTCOPY|CASTSEG|" + str(segment) + "|" + str(crchash) + "|" + b64data)
			# packet casted, wait for all receivers ack.
			s_usn = alive_count
			s_rcn = 0
			while True: 
				s_rcn += 1
				try: data, address = s.recvfrom(65535)
				finally:
					if data.decode('utf-8')[:17] == "CASTCOPY|CASTACK|":
						response = data.decode('utf-8').split("|")
						rpn_client_id = response[2]
						rpn_segment = response[3]
						try: clients_list[rpn_client_id]["acked_segments"]
						except: pass # Not my client, pass
						else:
							clients_list[rpn_client_id]["acked_segments"] == rpn_segment
							s_usn -= 1

					if data.decode('utf-8')[:17] == "CASTCOPY|CASTNAK|":
						try: clients_list[rpn_client_id]["acked_segments"]
						except: # Not my client, pass
							last_packet_done = False

					if s_rcn >= alive_count * 2 + 1:
						for rcvr in clients_list:
							if clients_list[rcvr]["acked_segments"] == segment:
								print(" ! Dead client " + rcvr)
								clients_list[rcvr]["dead_client"] == True
								alive_count -= 1
								s_usn -= 1

					if s_usn == 0:
						last_packet_done = True
						break

		else:
			castStr("CASTCOPY|CASTFINISHED")
			time_stopCast = time.time()
			print("\n - File casting completed. Total " + str(segment) + " segment(s)")
			print(" - Time elapsed: " + str(int(time_stopCast - time_startCast)) + " s")
			fh.close()
			s.close()
			quit(0)
