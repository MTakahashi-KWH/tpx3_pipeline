import socket
import time

HOST = "localhost"
SERVAL = 8088
BUFF_SIZE = 100 * 1024 * 1024
RETRY_ATTEMPTS = 10

def stream(sock, buffers, queues):
    free_q, full_q, out_q, file_q = queues
    flag_end = False
    index = 0
    while True:
        buf_i = free_q.get()  # take ownership
        buf = buffers[buf_i].buf
        # view = memoryview(buf)

        read_total = 0
        while (read_total < BUFF_SIZE * 0.8) or (read_total % 8 != 0):
            n = sock.recv_into(buf[read_total:])
            if n == 0:
                if flag_end:
                    break
                flag_end = True
                print(
                    "[socket]\t 0 packet received... suspecting eot... awaiting next update"
                )
            read_total += n
        print("[socket]\t writing out sequence ", index, " to buffer ", buf_i)
        # hand off buffer (no copy)
        full_q.put((buf_i, read_total - (read_total % 8), index))
        index += 1
        if flag_end:
            return


def trigger(buffers, queues):
    free_q, full_q, out_q, file_q = queues
    # Setup
    for i in range(RETRY_ATTEMPTS):
        print("[pipeline]\t connection attempt: ", i)
        try:
            print("... sucessful")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((HOST, SERVAL))
                # Start reader (main thread or separate)
                stream(sock,buffers,queues)
                full_q.join()
            return
        except OSError as e:
            print("... failed", e)
            time.sleep(0.05)


def socket_listener(trigger_e,buffers,queues):
    while True:
        trigger_e.wait()
        print("[pipeline]\t Triggered: connecting...")
        trigger(buffers,queues)
        trigger_e.clear()
