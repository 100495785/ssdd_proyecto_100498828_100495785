from enum import Enum
import argparse
import socket
import threading


class client:

    # ******************** TYPES *********************
    class RC(Enum):
        OK = 0
        ERROR = 1
        USER_ERROR = 2

    # ****************** ATTRIBUTES ******************
    _server = None
    _port = -1

    _connected_user = None
    _listen_socket = None
    _listen_port = None
    _listener_thread = None
    _stop_listener = False
    _print_lock = threading.Lock()

    # ******************** SOCKET UTILS *******************

    @staticmethod
    def _send_cstring(sock, text):
        sock.sendall(text.encode() + b'\0')

    @staticmethod
    def _recv_exact(sock, n):
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Socket closed")
            data += chunk
        return data

    @staticmethod
    def _recv_byte(sock):
        data = client._recv_exact(sock, 1)
        return data[0]

    @staticmethod
    def _recv_cstring(sock):
        data = bytearray()
        while True:
            c = sock.recv(1)
            if not c:
                raise ConnectionError("Socket closed")
            if c == b'\0':
                break
            data.extend(c)
        return data.decode()

    @staticmethod
    def _create_server_connection():
        return socket.create_connection((client._server, client._port))

    # ******************** LISTENER *******************

    @staticmethod
    def _listener_loop():
        while not client._stop_listener:
            try:
                conn, _addr = client._listen_socket.accept()
            except OSError:
                break

            try:
                op = client._recv_cstring(conn)

                if op == "SEND MESSAGE":
                    sender = client._recv_cstring(conn)
                    msg_id = client._recv_cstring(conn)
                    message = client._recv_cstring(conn)

                    with client._print_lock:
                        print(f"s> MESSAGE {msg_id} FROM {sender}")
                        print(message)
                        print("END")

                elif op == "SEND MESS_ACK":
                    msg_id = client._recv_cstring(conn)
                    with client._print_lock:
                        print(f"c> SEND MESSAGE {msg_id} OK")

            except Exception:
                pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    @staticmethod
    def _start_listener():
        client._listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client._listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client._listen_socket.bind(("", 0))
        client._listen_socket.listen(10)

        client._listen_port = client._listen_socket.getsockname()[1]
        client._stop_listener = False

        client._listener_thread = threading.Thread(target=client._listener_loop, daemon=True)
        client._listener_thread.start()

    @staticmethod
    def _stop_listener_thread():
        client._stop_listener = True

        if client._listen_socket is not None:
            try:
                client._listen_socket.close()
            except Exception:
                pass
            client._listen_socket = None

        if client._listener_thread is not None:
            try:
                client._listener_thread.join(timeout=1.0)
            except Exception:
                pass
            client._listener_thread = None

        client._listen_port = None

    # ******************** METHODS *******************

    @staticmethod
    def register(user):
        try:
            sock = client._create_server_connection()
            client._send_cstring(sock, "REGISTER")
            client._send_cstring(sock, user)

            code = client._recv_byte(sock)
            sock.close()

            if code == 0:
                print("c> REGISTER OK")
                return client.RC.OK
            elif code == 1:
                print("c> USERNAME IN USE")
                return client.RC.USER_ERROR
            else:
                print("c> REGISTER FAIL")
                return client.RC.ERROR

        except Exception:
            print("c> REGISTER FAIL")
            return client.RC.ERROR

    @staticmethod
    def unregister(user):
        try:
            sock = client._create_server_connection()
            client._send_cstring(sock, "UNREGISTER")
            client._send_cstring(sock, user)

            code = client._recv_byte(sock)
            sock.close()

            if code == 0:
                print("c> UNREGISTER OK")
                return client.RC.OK
            elif code == 1:
                print("c> USER DOES NOT EXIST")
                return client.RC.USER_ERROR
            else:
                print("c> UNREGISTER FAIL")
                return client.RC.ERROR

        except Exception:
            print("c> UNREGISTER FAIL")
            return client.RC.ERROR

    @staticmethod
    def connect(user):
        try:
            if client._connected_user is not None:
                print("c> USER ALREADY CONNECTED")
                return client.RC.USER_ERROR

            client._start_listener()

            sock = client._create_server_connection()
            client._send_cstring(sock, "CONNECT")
            client._send_cstring(sock, user)
            client._send_cstring(sock, str(client._listen_port))

            code = client._recv_byte(sock)
            sock.close()

            if code == 0:
                client._connected_user = user
                print("c> CONNECT OK")
                return client.RC.OK
            elif code == 1:
                client._stop_listener_thread()
                print("c> CONNECT FAIL, USER DOES NOT EXIST")
                return client.RC.USER_ERROR
            elif code == 2:
                client._stop_listener_thread()
                print("c> USER ALREADY CONNECTED")
                return client.RC.USER_ERROR
            else:
                client._stop_listener_thread()
                print("c> CONNECT FAIL")
                return client.RC.ERROR

        except Exception:
            client._stop_listener_thread()
            print("c> CONNECT FAIL")
            return client.RC.ERROR

    @staticmethod
    def users():
        try:
            if client._connected_user is None:
                print("c> CONNECTED USERS FAIL, USER IS NOT CONNECTED")
                return client.RC.USER_ERROR

            sock = client._create_server_connection()
            client._send_cstring(sock, "USERS")
            client._send_cstring(sock, client._connected_user)

            code = client._recv_byte(sock)

            if code == 0:
                n_str = client._recv_cstring(sock)
                n = int(n_str)
                users_list = []

                for _ in range(n):
                    users_list.append(client._recv_cstring(sock))

                sock.close()

                print(f"c> CONNECTED USERS ({n} users connected) OK")
                for u in users_list:
                    print(u)

                return client.RC.OK

            elif code == 1:
                sock.close()
                print("c> CONNECTED USERS FAIL, USER IS NOT CONNECTED")
                return client.RC.USER_ERROR

            else:
                sock.close()
                print("c> CONNECTED USERS FAIL")
                return client.RC.ERROR

        except Exception:
            print("c> CONNECTED USERS FAIL")
            return client.RC.ERROR

    @staticmethod
    def disconnect(user):
        try:
            sock = client._create_server_connection()
            client._send_cstring(sock, "DISCONNECT")
            client._send_cstring(sock, user)

            code = client._recv_byte(sock)
            sock.close()

            client._stop_listener_thread()
            client._connected_user = None

            if code == 0:
                print("c> DISCONNECT OK")
                return client.RC.OK
            elif code == 1:
                print("c> DISCONNECT FAIL, USER DOES NOT EXIST")
                return client.RC.USER_ERROR
            elif code == 2:
                print("c> DISCONNECT FAIL, USER NOT CONNECTED")
                return client.RC.USER_ERROR
            else:
                print("c> DISCONNECT FAIL")
                return client.RC.ERROR

        except Exception:
            client._stop_listener_thread()
            client._connected_user = None
            print("c> DISCONNECT FAIL")
            return client.RC.ERROR

    @staticmethod
    def send(user, message):
        try:
            if client._connected_user is None:
                print("c> SEND FAIL")
                return client.RC.ERROR

            if len(message) > 255:
                print("c> SEND FAIL")
                return client.RC.ERROR

            sock = client._create_server_connection()
            client._send_cstring(sock, "SEND")
            client._send_cstring(sock, client._connected_user)
            client._send_cstring(sock, user)
            client._send_cstring(sock, message)

            code = client._recv_byte(sock)

            if code == 0:
                msg_id = client._recv_cstring(sock)
                sock.close()
                print(f"c> SEND OK - MESSAGE {msg_id}")
                return client.RC.OK
            elif code == 1:
                sock.close()
                print("c> SEND FAIL, USER DOES NOT EXIST")
                return client.RC.USER_ERROR
            else:
                sock.close()
                print("c> SEND FAIL")
                return client.RC.ERROR

        except Exception:
            print("c> SEND FAIL")
            return client.RC.ERROR

    @staticmethod
    def sendAttach(user, file, message):
        print("SENDATTACH not implemented in part 1")
        return client.RC.ERROR

    # ******************** SHELL *********************
    @staticmethod
    def shell():
        while True:
            try:
                command = input("c> ")
                line = command.split(" ")

                if len(line) > 0:
                    line[0] = line[0].upper()

                    if line[0] == "REGISTER":
                        if len(line) == 2:
                            client.register(line[1])
                        else:
                            print("Syntax error. Usage: REGISTER <userName>")

                    elif line[0] == "UNREGISTER":
                        if len(line) == 2:
                            client.unregister(line[1])
                        else:
                            print("Syntax error. Usage: UNREGISTER <userName>")

                    elif line[0] == "CONNECT":
                        if len(line) == 2:
                            client.connect(line[1])
                        else:
                            print("Syntax error. Usage: CONNECT <userName>")

                    elif line[0] == "DISCONNECT":
                        if len(line) == 2:
                            client.disconnect(line[1])
                        else:
                            print("Syntax error. Usage: DISCONNECT <userName>")

                    elif line[0] == "USERS":
                        if len(line) == 1:
                            client.users()
                        else:
                            print("Syntax error. Usage: USERS")

                    elif line[0] == "SEND":
                        if len(line) >= 3:
                            message = ' '.join(line[2:])
                            client.send(line[1], message)
                        else:
                            print("Syntax error. Usage: SEND <userName> <message>")

                    elif line[0] == "SENDATTACH":
                        if len(line) >= 4:
                            message = ' '.join(line[3:])
                            client.sendAttach(line[1], line[2], message)
                        else:
                            print("Syntax error. Usage: SENDATTACH <userName> <filename> <message>")

                    elif line[0] == "QUIT":
                        if len(line) == 1:
                            if client._connected_user is not None:
                                client._stop_listener_thread()
                                client._connected_user = None
                            break
                        else:
                            print("Syntax error. Use: QUIT")
                    else:
                        print("Error: command " + line[0] + " not valid.")

            except Exception as e:
                print("Exception: " + str(e))

    # ******************** ARGUMENTS *********************
    @staticmethod
    def usage():
        print("Usage: python3 client.py -s <server> -p <port>")

    @staticmethod
    def parseArguments(argv):
        parser = argparse.ArgumentParser()
        parser.add_argument('-s', type=str, required=True, help='Server IP')
        parser.add_argument('-p', type=int, required=True, help='Server Port')
        args = parser.parse_args()

        if args.s is None:
            parser.error("Usage: python3 client.py -s <server> -p <port>")
            return False

        if args.p < 1024 or args.p > 65535:
            parser.error("Error: Port must be in the range 1024 <= port <= 65535")
            return False

        client._server = args.s
        client._port = args.p

        return True

    # ******************** MAIN *********************
    @staticmethod
    def main(argv):
        if not client.parseArguments(argv):
            client.usage()
            return

        client.shell()
        print("+++ FINISHED +++")


if __name__ == "__main__":
    client.main([])