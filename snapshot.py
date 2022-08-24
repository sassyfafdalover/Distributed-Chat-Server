from tracemalloc import Snapshot
from sqlalchemy import true
from mpi4py import MPI
import linecache
import time
import random
import string

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
leader = False
curleader = -1
marked = False
messege_sent_count = {}
messege_recieved_count = {}
messege_sent_count_marker = {}
messege_recieved_count_marker = {}
messege_channel_marker = {}


def random_string_gen(size=10, chars=string.ascii_uppercase + string.digits):
    nouns = ("puppy", "car", "rabbit", "girl", "monkey")
    verbs = ("runs", "hits", "jumps", "drives", "barfs")
    adv = ("crazily.", "dutifully.", "foolishly.", "merrily.", "occasionally.")
    adj = ("adorable", "clueless", "dirty", "odd", "stupid")
    num = random.randrange(0, 5)
    num2 = random.randrange(0, 5)
    num3 = random.randrange(0, 5)
    num4 = random.randrange(0, 5)
    # sentence is randomly generated from given
    return nouns[num] + ' ' + verbs[num2] + ' ' + adj[num3] + ' ' + adv[num4]        # components of speech


def genfile():
    global size  # get total processes in world
    file = open("input.txt", "w")
    # decide number of processes to participate in chat
    n = random.randint(2, size)
    strtowrite = ""
    # randomly decide list of processes to participate in chat
    chatlist = random.sample(range(0, size), n)
    chatlist.sort()
    for i in range(len(chatlist)):  # Build string to write
        strtowrite = strtowrite + str(chatlist[i]) + " "

    strtowrite = strtowrite + "\n"
    file.write(strtowrite)  # write to file
    t = 7

    for i in range(t):  # generate random chat messages and write to file
        strtowrite = ""
        sender = random.choice(chatlist)
        strtowrite = strtowrite + str(sender) + " : "
        strtowrite = strtowrite + random_string_gen()
        strtowrite = strtowrite + "\n"
        file.write(strtowrite)

    file.write('END\n')  # End file
    file.write('ENDFILE')


def elect_leader():
    global leader
    global curleader
    color = 'red'  # initially process colour is red
    tosend = rank+1  # process has to send to its immediate sucessive rank
    if(tosend == size):  # if last process, connect to first to forma a circular ring.
        tosend = 0

    if(rank == 0):
        token = 0
        # send token with process number, 0 is the root process initialising
        comm.send(token, dest=tosend)
        print('Sent ', token)  # leadership election
        time.sleep(1)
    else:
        # receive token from preceeding process
        token = comm.recv(source=rank-1)
        print('Received ', token)
        time.sleep(1)
        if(rank > token):  # if current rank of process is greater than rank of token
            color = 'black'  # process color's itself black
            token = rank  # process assign itself as token
        comm.send(token, dest=tosend)  # send token to next process
        print('Sent ', token)
        time.sleep(1)

    torecv = rank - 1
    if(rank == 0):
        torecv = size - 1
    token = comm.recv(source=torecv)  # receive token again
    print('Received ', token)
    time.sleep(1)
    curleader = token
    if(token == rank):  # if token and rank are same, process is elected as leader
        print('Leader elected!')
        leader = True
    comm.send(token, dest=tosend)  # send token to next process
    if(rank == 0):
        token = comm.recv(source=torecv)  # 0 receives token and terminates
        time.sleep(1)


def leaderfunc():
    curline = 1
    # Leader first reads the file and retrieves the chatlist
    data = linecache.getline('input.txt', curline)

    while(data):
        chatlist = data.split()
        if(len(chatlist) > size):  # if chatlist is greater than world size, terminate
            print('World is smaller than total chat members!')
            break
        for i in range(len(chatlist)):  # send chatlist and line to start from
                                                                 # to all processes in chatlist
            if(not(int(chatlist[i]) == rank)):
                comm.send(data, dest=int(chatlist[i]))
                comm.send(curline+1, dest=int(chatlist[i]))
                time.sleep(1)

        if(str(rank) in chatlist):
            # If leader is a process in chatlist, send asynchronously
            comm.isend(data, dest=int(chatlist[i]))
            comm.isend(curline+1, dest=int(chatlist[i]))
            managemessage()

        # When chat ends, leader receieves line to continue from
        recline = comm.recv(source=MPI.ANY_SOURCE)
        time.sleep(1)
        curline = recline
        # print(recline)
        data = linecache.getline('input.txt', recline)
        if(data[0] == 'E'):  # If ENDFILE is read, terminate all processes
            for i in range(size):
                if(not(i == rank)):
                    comm.send('ENDFILE', i)
            print('Finished execution')
            break

# functiin used to add messege count into process send count buffer


def addtosentcount(sendto):
    global messege_sent_count
    if not (sendto in messege_sent_count):
        messege_sent_count[sendto] = 1
    else:
        messege_sent_count[sendto] += 1

# functiin used to add messege count into process recieve count buffer


def addtorecievecount(recievedfrom):
    global messege_recieved_count
    if not (recievedfrom in messege_recieved_count):
        messege_recieved_count[recievedfrom] = 1
    else:
        messege_recieved_count[recievedfrom] += 1

# function used to send marker to other processes if marker is recieved first time


def sendmarker(chatlist):
    global marked, messege_sent_count_marker, messege_sent_count, messege_recieved_count, messege_recieved_count_marker
    # saving snapshot at current instant
    messege_sent_count_marker = messege_sent_count
    messege_recieved_count_marker = messege_recieved_count
    # sending marker to other processes
    for process in chatlist:
        if not (int(process) == rank):
            data = "||marker||"+" "+str(rank)
            comm.send(data, dest=int(process))
            time.sleep(1)
    marked = True

# function used to collect snapshot after it's current snapshot is taken by each process


def collectsnapshot(chatlist):
    file = open("snapshot.txt", "w+")
    # sending signal to collect snapshot
    for process in chatlist:
        if not (int(process) == rank):
            data = "||collect||"+" "+str(rank)
            comm.send(data, dest=int(process))
            time.sleep(1)
    # storing the snapshot of initiator
    finalsnapshot = "Process " + str(rank) + " snapshot" + "\n"
    for count in messege_sent_count_marker.keys():
        finalsnapshot += "Sent " + \
            str(messege_sent_count_marker[count]) + \
            " messegaes to "+str(count)+"\n"

    for count in messege_recieved_count_marker.keys():
        finalsnapshot += "Recieved " + \
            str(messege_recieved_count_marker[count]
                )+" messeges from "+str(count)+"\n"

    for count in messege_channel_marker.keys():
        finalsnapshot += "Messeges in channel " + \
            str(rank)+" - "+str(count)+" : " + \
            str(messege_channel_marker[count])+"\n"
    finalsnapshot += "\n"
    # storing snapshot recieved from other processes after recieveing signal
    for process in chatlist:
        if not (int(process) == rank):
            data = comm.recv(source=int(process))
            time.sleep(1)
            finalsnapshot += "Process " + process + " snapshot" + "\n"
            finalsnapshot += str(data)+"\n"
    # stroing snapshot in snapshot.txt file
    file.write(finalsnapshot)

# function used to send snapshot from other process to initiator


def sendsnapshot(destination):
    # collecting snapshot of process in snapshot string
    snapshot = ""
    for count in messege_sent_count_marker.keys():
        snapshot += "Sent " + \
            str(messege_sent_count_marker[count]) + \
            " messegaes to "+str(count)+"\n"
    for count in messege_recieved_count_marker.keys():
        snapshot += "Recieved " + \
            str(messege_recieved_count_marker[count]
                )+" messeges from "+str(count)+"\n"
    for count in messege_channel_marker.keys():
        snapshot += "Messeges in channel " + \
            str(rank)+" - "+str(count)+" : " + \
            str(messege_channel_marker[count])+"\n"

    # sending snapshot to initiator process
    comm.send(snapshot, dest=destination)
    time.sleep(1)


def sendmessage(chatlist, recline):
    global marked, messege_recieved_count, messege_recieved_count_marker, messege_channel_marker
    while(True):
        data = linecache.getline('input.txt', recline)
        if(data[0] == 'E'):
            for i in range(len(chatlist)):
                if(not(int(chatlist[i]) == rank)):
                    comm.send(data, dest=int(chatlist[i]))
                    time.sleep(1)
            # initiating snapshot algorithm by last process
            sendmarker(chatlist)
            # waiting of some time for completion of simulation of algorithm
            time.sleep(3)
            # collecting snapshot recieved from other processes
            for i in range(len(chatlist)):
                if(not (int(chatlist[i]) == rank)):
                    data = comm.recv(source=int(chatlist[i]))
                if data[0:10] == "||marker||":
                    if marked == False:
                        sendmarker(chatlist)
                    else:
                        marker_from = int(data[11])
                        messege_channel_marker[marker_from] = messege_recieved_count[marker_from] - \
                            messege_recieved_count_marker[marker_from]
            collectsnapshot(chatlist)

            print('Finished chat')
            if(not(rank == curleader)):
                comm.send(recline+1, dest=curleader)
                time.sleep(1)
            else:
                comm.isend(recline+1, dest=curleader)
            break
        if(not(int(data[0]) == rank)):
            comm.send(recline, dest=int(data[0]))
            time.sleep(1)
            print('Handing chat to', data[0])
            recvmessage(chatlist)
            break
        else:
            for i in range(len(chatlist)):
                if(not(int(chatlist[i]) == rank)):
                    print(rank, "sends", data[4:])
                    comm.send(data, dest=int(chatlist[i]))
                    time.sleep(1)
                    # adding send count for the current process
                    addtosentcount(int(chatlist[i]))
        recline = recline + 1


def recvmessage(chatlist):
    global marked, messege_recieved_count, messege_recieved_count_marker, messege_channel_marker
    while(True):
        data = comm.recv(source=MPI.ANY_SOURCE)
        time.sleep(1)

        if(isinstance(data, int)):
            recline = int(data)
            sendmessage(chatlist, recline)
            break
        else:
            if(data[0] == 'E'):
                print('Finished chat')
                # break
            # condition to recieve marker
            elif data[0:10] == "||marker||":
                # recieved first time
                if marked == False:
                    sendmarker(chatlist)
                # recievd other than first time so present in channel
                else:
                    marker_from = int(data[11])
                    messege_channel_marker[marker_from] = messege_recieved_count[marker_from] - \
                        messege_recieved_count_marker[marker_from]
                # recieved signal from initiator to collect messege
            elif data[0:11] == "||collect||":
                sendsnapshot(int(data[12]))
                break
            else:
                # adding recieve count for current processs
                addtorecievecount(int(data[0]))
                print(data)


def managemessage():
    while(True):
        reclist = comm.recv(source=curleader)
        time.sleep(1)
        print(reclist)
        if(reclist[0] == 'E'):
            print('Finished execution')
            break
        chatlist = reclist.split()
        for i in range(len(chatlist)):
            messege_recieved_count[int(chatlist[i])] = 0
            messege_sent_count[int(chatlist[i])] = 0
            messege_channel_marker[int(chatlist[i])] = 0
        recline = comm.recv(source=curleader)
        time.sleep(1)
        # print(recline)

        data = linecache.getline('input.txt', recline)
        recline = recline + 1

        if(int(data[0]) == rank):
            print(data[0], 'sends', data[4:])
            for i in range(len(chatlist)):
                if(not(int(chatlist[i]) == rank)):
                    addtosentcount(int(chatlist[i]))
            sendmessage(chatlist, recline)
        else:
            print(data)
            addtorecievecount(int(data[0]))
            recvmessage(chatlist)

        if(rank == curleader):
            break


elect_leader()                #Elect leader
if(leader == True):
    genfile()
    leaderfunc()              #IF leader execute leaderfunc
else:
    managemessage()           #Else go to managemessage()
