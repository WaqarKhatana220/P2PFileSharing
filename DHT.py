import socket
import threading
import os
import time
import hashlib
from json import dumps, loads

class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		# You will need to kill this thread when leaving, to do so just set self.stop = True
		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = []
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))
		'''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		'''
		# Set value of the following variables appropriately to pass Intialization test
		self.predecessor = (host, port)
		self.successor = (host, port)
		self.highestHash = self.hasher(self.host+str(self.port))
		self.lowestHash = self.hasher(self.host+str(self.port))

		# additional state variables



	def hasher(self, key):
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N


	def handleConnection(self, client, addr):
		'''
		 Function to handle each inbound connection, called as a thread from the listener.
		'''
		# try:
		msg = client.recv(2056)
		# print("messageHere", msg)
		# print("client", client, "addr", addr)

		if msg:
			msg = loads(msg)
			# print("message", msg)
			if type(msg) == int:
				
				print("yes")
				# self.recieveFile(client, x)
				# print("received")
			# print("msg", msg)
			else:
				msgType = msg["type"]
				if msgType == "abMereKoToAndarLo":

					host, port = msg["addr"]
					incomingAddr = (host, port)
					myHash = self.hasher(self.host+str(self.port))
					mySuccessorHash = self.hasher(self.successor[0]+str(self.successor[1]))
					myPredecessorHash = self.hasher(self.predecessor[0]+str(self.predecessor[1]))
					incomingNodeHash = self.hasher(host+str(port))

					if myHash == mySuccessorHash and myHash == myPredecessorHash:
						self.successor = incomingAddr
						self.predecessor = incomingAddr					
						msg = {
						"type":"yourNeighbors",
						"successor": (self.host, self.port),
						"predecessor":(self.host, self.port)
						}
					else:
						# print("self.addr", (self.host, self.port), "self.successor", self.successor, "self.predecessor", self.predecessor)
						highestHash, haddr, hsuccessor, hpredecessor = self.findHighest()
						lowestHash, laddr, lsuccessor, lpredecessor = self.findLowest()
						if incomingNodeHash > highestHash:
							msg = {
								"type":"yourNeighbors",
								"successor": hsuccessor,
								"predecessor":haddr
								}
						elif incomingNodeHash < lowestHash:
							msg = {
								"type":"yourNeighbors",
								"successor": laddr,
								"predecessor":lpredecessor
								}
						else:
							successorHost, successorPort, predecessorHost, predecessorPort = self.lookup(incomingAddr)
							# print("successorHost", successorHost,"successorPort", successorPort)
							if self.host == successorHost and self.port == successorPort:
								msg = {
									"type":"yourNeighbors",
									"successor": (self.host, self.port),
									"predecessor":self.predecessor
									}
								self.predecessor = (host, port)
							else:
								msg = {
									"type":"yourNeighbors",
									"successor": (successorHost, successorPort),
									"predecessor":(predecessorHost, predecessorPort)
									}
				elif msgType == "updateYourSuccessor":
					successorHost, successorPort = msg["successor"]
					self.successor = (successorHost, successorPort)
				elif msgType == "updateYourPredecessor":
					predecessorHost, predecessorPort = msg["predecessor"]
					self.predecessor = (predecessorHost, predecessorPort)


				elif msgType == "sendingFile":
					fileName = msg["fileName"]
					print("receiving file initiated")
					fileName = "localhost_"+str(self.port)+"/"+fileName
					# directory = os.path.join(directory, fileName)
					# print("directory", directory)
					print("receiveing file", fileName)
					self.recieveFile(client, fileName)
					print("received")




				elif msgType == "findItsSuccessor":
					print("type heere", type(msg["addr"]))
					if type(msg["addr"]) == list:
						host, port = msg["addr"]
						successorHost, successorPort, predecessorHost, predecessorPort = self.lookup((host, port))
						if self.host == successorHost and self.port == successorPort:
							msg = {
								"type":"hisNeighbors",
								"successor": (self.host, self.port),
								"predecessor":self.predecessor
								}
							self.predecessor = (host, port)
						else:
							msg = {
								"type":"hisNeighbors",
								"successor": (successorHost, successorPort),
								"predecessor":(predecessorHost, predecessorPort)
							}
					
					else:
						fileName = msg["addr"]
						successorHost, successorPort, predecessorHost, predecessorPort = self.lookup(fileName)
						msg = {
							"type":"hisNeighbors",
							"successor": (successorHost, successorPort),
							"predecessor":(predecessorHost, predecessorPort)
						}
				elif msgType == "whoHasHighestHash":
					myHash = self.hasher(self.host+str(self.port))
					highestHash, highestHashAddr, successor, predecessor = self.findHighest()
					msg = {
						"type":"highestHash",
						"value":highestHash,
						"addr":highestHashAddr,
						"successor":successor,
						"predecessor":predecessor
					}

				elif msgType == "whoHasLowestHash":
					myHash = self.hasher(self.host+str(self.port))
					lowestHash, lowestHashAddr, successor, predecessor = self.findLowest()
					msg = {
						"type":"lowestHash",
						"value":lowestHash,
						"addr":lowestHashAddr,
						"successor":successor,
						"predecessor":predecessor
					}
						



				msg = dumps(msg)
				client.send(msg.encode('utf-8'))
				client.close()




		# except Exception as e:
		# 	print("exception raiseded", e)



	def listener(self):
		'''
		We have already created a listener for you, any connection made by other nodes will be accepted here.
		For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
		to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
		'''
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()

	def join(self, joiningAddr):
		'''
		This function handles the logic of a node joining. This function should do a lot of things such as:
		Update successor, predecessor, getting files, back up files. SEE MANUAL FOR DETAILS.
		'''

		if joiningAddr == "":
			self.successor = (self.host, self.port)
			self.predecessor = (self.host, self.port)

		else:
			host, port = joiningAddr
			
			try:
				sock = socket.socket()
				sock.connect(joiningAddr)
				msg = {
					"type":"abMereKoToAndarLo",
					"addr":(self.host, self.port)
				}
				msgSend = dumps(msg)
				try:
					sock.sendto(msgSend.encode('utf-8'), (host, port))
				except Exception as e:
					print("socket communication error:", e)

				msgRecv = loads(sock.recv(1024))
				msgType = msgRecv["type"]
				if msgType == "yourNeighbors":
					# print("neighbors received")
					successorHost, successorPort = msgRecv["successor"]
					predecessorHost, predecessorPort = msgRecv["predecessor"]
					self.successor = (successorHost, successorPort)
					self.predecessor = (predecessorHost, predecessorPort)
					self.tellPredecessor(self.predecessor)
					self.tellSuccessor(self.successor)
				time.sleep(2)
				sock.close()
			except Exception as e:
				print("socket eror", e)





	def tellPredecessor(self, predecessor):
		sock = socket.socket()
		sock.connect(predecessor)
		msg = {
			"type":"updateYourSuccessor",
			"successor":(self.host, self.port)
		}
		msg = dumps(msg)
		try:
			sock.sendto(msg.encode('utf-8'), predecessor)
		except Exception as e:
			print("socket error", e)
		sock.close()

	def tellSuccessor(self, successor):
		sock = socket.socket()
		sock.connect(successor)
		msg = {
			"type":"updateYourPredecessor",
			"predecessor":(self.host, self.port)
		}
		msg = dumps(msg)
		try:
			sock.sendto(msg.encode('utf-8'), successor)
		except Exception as e:
			print("socket error", e)
		sock.close()

	def findHighest(self):
		myHash = self.hasher(self.host+str(self.port))
		mySuccessorHash = self.hasher(self.successor[0]+str(self.successor[1]))
		if myHash >= mySuccessorHash:
			return myHash, (self.host, self.port), self.successor, self.predecessor
		
		else:
			sock = socket.socket()
			sock.connect(self.successor)
			msg = {
				"type":"whoHasHighestHash"
			}
			
			msgSend = dumps(msg)
			sock.sendto(msgSend.encode('utf-8'), self.successor)
			try:
				msgRcv = loads(sock.recv(2056))
			except:
				print("no message to rcv")
			msgType = msgRcv["type"]
			if msgType == "highestHash":
				highestHash = msgRcv["value"]
				host, port = msgRcv["addr"]
				successorHost, successorPort = msgRcv["successor"]
				predecessorHost, predecessorPort = msgRcv["predecessor"]
				return highestHash, (host, port), (successorHost, successorPort), (predecessorHost, predecessorPort)

	def findLowest(self):
		myHash = self.hasher(self.host+str(self.port))
		myPredecessorHash = self.hasher(self.predecessor[0]+str(self.predecessor[1]))
		if myHash <= myPredecessorHash:
			return myHash, (self.host, self.port), self.successor, self.predecessor
		
		else:
			sock = socket.socket()
			sock.connect(self.predecessor)
			msg = {
				"type":"whoHasLowestHash"
			}
			
			msgSend = dumps(msg)
			sock.sendto(msgSend.encode('utf-8'), self.predecessor)
			try:
				msgRcv = loads(sock.recv(2056))
			except:
				print("no message to rcv")
			msgType = msgRcv["type"]
			if msgType == "lowestHash":
				lowestHash = msgRcv["value"]
				host, port = msgRcv["addr"]
				successorHost, successorPort = msgRcv["successor"]
				predecessorHost, predecessorPort = msgRcv["predecessor"]
				return lowestHash, (host, port), (successorHost, successorPort), (predecessorHost, predecessorPort)

	def lookup(self, incomingAddr):
		#I am the only node in the network
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		print("type", type(incomingAddr))
		myHash = self.hasher(self.host+str(self.port))
		mySuccessorHash = self.hasher(self.successor[0]+str(self.successor[1]))
		myPredecessorHash = self.hasher(self.predecessor[0]+str(self.predecessor[1]))
		if type(incomingAddr) == tuple:
			host, port = incomingAddr
			incomingNodeHash = self.hasher(host+str(port))

		elif type(incomingAddr) == str:
			incomingNodeHash = self.hasher(incomingAddr)
		# print("myHash", myHash, "nodeHash", incomingNodeHash, "predecessorHash", myPredecessorHash)
		if incomingNodeHash < myHash and incomingNodeHash > myPredecessorHash: # i am his successor
			# print("found")
			predHost, predPort = self.predecessor
			return(self.host, self.port, predHost, predPort)
		else:
			# print("1")
			sock = socket.socket()
			sock.connect(self.successor)
			# print("2")

			msg = {
				"type":"findItsSuccessor",
				"addr":incomingAddr
			}
			# print("3")
			msg = dumps(msg)
			sock.sendto(msg.encode('utf-8'), self.successor)
			# print("4")

			msg = loads(sock.recv(2056))
			msgType = msg["type"]
			# print("5")

			if msgType == 'hisNeighbors':
				successorHost, successorPort = msg["successor"]
				predHost, predPort = msg["predecessor"]
				return(successorHost, successorPort, predHost, predPort)

			sock.close()







	def put(self, fileName):
		'''
		This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
		Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
		in directory given by host_port e.g. "localhost_20007/file.py".
		'''
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		print("put file", fileName)
		highestHash, haddr, hsuccessor, hpredecessor = self.findHighest()
		lowestHash, laddr, lsuccessor, lpredecessor = self.findLowest()
		fileHash = self.hasher(fileName)
		if fileHash > highestHash:
			sock = socket.socket()
			sock.connect(hsuccessor)
			msg = {
				"type":"sendingFile",
				"fileName":fileName
			}
			msgSend = dumps(msg)
			sock.sendto(msgSend.encode('utf-8'), hsuccessor)
			# time.sleep(2)
			# directory = "/localhost_"+str(hsuccessor[1])
			# fileName = os.path.join(directory, fileName)
			self.sendFile(sock, fileName)
			print("sending")
			sock.close()
			print("sent 1")
		elif fileHash < lowestHash:
			sock = socket.socket()
			sock.connect(lsuccessor)
			msg = {
				"type":"sendingFile",
				"fileName":fileName
			}
			msgSend = dumps(msg)
			sock.sendto(msgSend.encode('utf-8'), lsuccessor)
			# time.sleep(2)
			directory = "/localhost_"+str(lsuccessor[1])
			# fileName = os.path.join(directory, fileName)
			self.sendFile(sock, fileName)
			print("sending")
			sock.close()
			print("sent 2")
		else:
			successorHost, successorPort, predecessorHost, predecessorPort = self.lookup(fileName)
			sock = socket.socket()
			sock.connect((successorHost, successorPort))
			msg = {
				"type":"sendingFile",
				"fileName":fileName
			}
			msgSend = dumps(msg)
			sock.sendto(msgSend.encode('utf-8'), (successorHost, successorPort))
			#time.sleep(2)
			directory = "/localhost_"+str(successorPort)
			# fileName = os.path.join(directory, fileName)
			self.sendFile(sock, fileName)
			print("sending")
			sock.close()
			print("sent 1")


	def get(self, fileName):
		'''
		This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
		i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
		'''


	def leave(self):
		'''
		When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
		it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
		by setting self.stop flag to True
		'''

	def sendFile(self, soc, fileName):
		'''
		Utility function to send a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		soc.recv(1024).decode('utf-8')
		with open(fileName, "rb") as file:
			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				soc.send(contentChunk)
				contentChunk = file.read(1024)

	def recieveFile(self, soc, fileName):
		'''
		Utility function to recieve a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = int(soc.recv(1024).decode('utf-8'))
		soc.send("ok".encode('utf-8'))
		contentRecieved = 0
		file = open(fileName, "wb")
		while contentRecieved < fileSize:
			contentChunk = soc.recv(1024)
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()

	def kill(self):
		# DO NOT EDIT THIS, used for code testing
		self.stop = True


