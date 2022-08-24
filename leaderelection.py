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


def sendmessage(chatlist, recline):
    while(True):
        data = linecache.getline('input.txt', recline)
        if(data[0] == 'E'):
            for i in range(len(chatlist)):
                if(not(int(chatlist[i]) == rank)):
                    comm.send(data, dest=int(chatlist[i]))
                    time.sleep(1)

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
        recline = recline + 1


def recvmessage(chatlist):
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
                break
            else:
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
        recline = comm.recv(source=curleader)
        time.sleep(1)
        # print(recline)

        data = linecache.getline('input.txt', recline)
        recline = recline + 1

        if(int(data[0]) == rank):
            print(data[0], 'sends', data[4:])
            sendmessage(chatlist, recline)
        else:
            print(data)
            recvmessage(chatlist)

        if(rank == curleader):
            break


elect_leader()  # Elect leader
if(leader == True):
    genfile()
    leaderfunc()  # IF leader execute leaderfunc
else:
    managemessage()  # Else go to managemessage()
