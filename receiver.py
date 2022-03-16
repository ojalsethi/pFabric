import sys
import socket
import time
import threading


class ReceiverClass():

    def __init__(self):
        self.receive_str = ""

    def listen(self, rcv_port, exp_time):
        TIMEOUT = exp_time + 2
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', rcv_port))
        # s.settimeout(TIMEOUT)
        s.listen(128)

        start = time.time()
        while True:  # (time.time()-start) < TIMEOUT:
            try:
                conn, addr = s.accept()
                t = threading.Thread(target=self.handleClient, args=(conn, addr))
                t.start()
            except socket.error:
                continue
            
        s.close()

    def handleClient(self, connection, addr):
        while 1:
            data = connection.recv(1024)
            if not data:
                break
        connection.close()


def main():
    rcv_port = int(sys.argv[1])
    exp_time = int(sys.argv[3])

    receiver_obj = ReceiverClass()

    receiver_obj.listen(rcv_port, exp_time)

if __name__ == '__main__':
    main()


