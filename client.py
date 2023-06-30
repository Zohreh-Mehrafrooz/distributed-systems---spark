# done by Zohreh Mehrafrooz , Id: 40218210
import os
import socket
from pathlib import Path

# you should specify a folder directory for dl and ul in client side here,
# otherwise it considers the os.getcwd
# uldl_directory_client = Path('C:/Users/zohre/PycharmProjects/TestA1ForSubmit/client')
uldl_directory_client = os.getcwd()


def receive_message_ending_with_token(active_socket, buffer_size, eof_token):
    """
    Same implementation as in receive_message_ending_with_token() in server.py
    A helper method to receives a bytearray message of arbitrary size sent on the socket.
    This method returns the message WITHOUT the eof_token at the end of the last packet.
    :param active_socket: a socket object that is connected to the server
    :param buffer_size: the buffer size of each recv() call
    :param eof_token: a token that denotes the end of the message.
    :return: a bytearray message with the eof_token stripped from the end.
    """
    file_content = bytearray()
    while True:
        packet = active_socket.recv(buffer_size)
        if packet[-10:] == eof_token.encode():
            file_content += packet[:-10]
            break
        file_content += packet
    return file_content


def initialize(host, port):
    """
    1) Creates a socket object and connects to the server.
    2) receives the random token (10 bytes) used to indicate end of messages.
    3) Displays the current working directory returned from the server (output of get_working_directory_info() at the server).
    Use the helper method: receive_message_ending_with_token() to receive the message from the server.
    :param host: the ip address of the server
    :param port: the port number of the server
    :return: the created socket object
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    print('Connected to server at IP:', host, 'and Port:', port)

    # receive token from server
    eof_token = s.recv(1024)
    print('Handshake Done. EOF is:', eof_token.decode())

    # receive working_directory from server
    message_get_cwd = s.recv(1024)
    user_working_directory = input(message_get_cwd.decode())
    s.sendall(user_working_directory.encode())
    working_directory_info = receive_message_ending_with_token(s, 1024, eof_token.decode())
    print(f"Server sends this working_directory_info: {working_directory_info.decode()}")
    return s, eof_token.decode()


def issue_cd(command_and_arg, client_socket, eof_token):
    """
    Sends the full cd command entered by the user to the server. The server changes its cwd accordingly and sends back
    the new cwd info.
    Use the helper method: receive_message_ending_with_token() to receive the message from the server.
    :param command_and_arg: full command (with argument) provided by the user.
    :param client_socket: the active client socket object.
    :param eof_token: a token to indicate the end of the message.
    """
    message = command_and_arg + eof_token
    print('client sent this message: ', message)
    client_socket.sendall(message.encode())
    working_directory_info = receive_message_ending_with_token(client_socket, 1024, eof_token)
    print(f"Server sends this working_directory_info: {working_directory_info.decode()}")


def issue_mkdir(command_and_arg, client_socket, eof_token):
    """
    Sends the full mkdir command entered by the user to the server. The server creates the sub directory and sends back
    the new cwd info.
    Use the helper method: receive_message_ending_with_token() to receive the message from the server.
    :param command_and_arg: full command (with argument) provided by the user.
    :param client_socket: the active client socket object.
    :param eof_token: a token to indicate the end of the message.
    """
    message = command_and_arg + eof_token
    print('client sent this message: ', message)
    client_socket.sendall(message.encode())
    working_directory_info = receive_message_ending_with_token(client_socket, 1024, eof_token)
    print(f"Server sends this working_directory_info: {working_directory_info.decode()}")


def issue_rm(command_and_arg, client_socket, eof_token):
    """
    Sends the full rm command entered by the user to the server. The server removes the file or directory and sends back
    the new cwd info.
    Use the helper method: receive_message_ending_with_token() to receive the message from the server.
    :param command_and_arg: full command (with argument) provided by the user.
    :param client_socket: the active client socket object.
    :param eof_token: a token to indicate the end of the message.
    """
    message = command_and_arg + eof_token
    print('client sent this message: ', message)
    client_socket.sendall(message.encode())
    working_directory_info = receive_message_ending_with_token(client_socket, 1024, eof_token)
    print(f"Server sends this working_directory_info: {working_directory_info.decode()}")


def issue_ul(command_and_arg, client_socket, eof_token):
    """
    Sends the full ul command entered by the user to the server. Then, it reads the file to be uploaded as binary
    and sends it to the server. The server creates the file on its end and sends back the new cwd info.
    Use the helper method: receive_message_ending_with_token() to receive the message from the server.
    :param command_and_arg: full command (with argument) provided by the user.
    :param client_socket: the active client socket object.
    :param eof_token: a token to indicate the end of the message.
    """
    message = command_and_arg + eof_token
    print('client sent this message: ', message)
    client_socket.sendall(message.encode())

    file_name = command_and_arg[3:]
    print('file_name is: ', file_name)

    file_path = os.path.join(uldl_directory_client, file_name)
    with open(file_path, 'rb') as f:
        file_content = f.read()

    file_content_with_token = file_content + eof_token.encode()
    client_socket.sendall(file_content_with_token)

    print('file uploaded successfully.')

    working_directory_info = receive_message_ending_with_token(client_socket, 1024, eof_token)
    print(f"Server sends this working_directory_info: {working_directory_info.decode()}")


def issue_dl(command_and_arg, client_socket, eof_token):
    """
    Sends the full dl command entered by the user to the server. Then, it receives the content of the file via the
    socket and re-creates the file in the local directory of the client. Finally, it receives the latest cwd info from
    the server.
    Use the helper method: receive_message_ending_with_token() to receive the message from the server.
    :param command_and_arg: full command (with argument) provided by the user.
    :param client_socket: the active client socket object.
    :param eof_token: a token to indicate the end of the message.
    :return:"""
    message = command_and_arg + eof_token
    print('client sent this message: ', message)
    client_socket.sendall(message.encode())

    file_content = receive_message_ending_with_token(client_socket, 1024, eof_token)
    print('file content received')

    path = os.path.join(uldl_directory_client, command_and_arg[3:])
    with open(path, 'wb') as file:
        file.write(file_content)

    working_directory_info = receive_message_ending_with_token(client_socket, 1024, eof_token)
    print(f"Server sends this working_directory_info: {working_directory_info.decode()}")


def main():

    HOST = "127.0.0.1"  # The server's hostname or IP address
    PORT = 65432  # The port used by the server

    # initialize
    client_socket, client_token = initialize(HOST, PORT)
    while True:
        # get user input
        user_input = input('Enter your command')

        # call the corresponding command function or exit
        if user_input[0:2] == 'cd':
            issue_cd(user_input, client_socket, client_token)
        elif user_input[0:2] == 'mk':
            issue_mkdir(user_input, client_socket, client_token)
        elif user_input[0:2] == 'rm':
            issue_rm(user_input, client_socket, client_token)
        elif user_input[0:2] == 'ul':
            issue_ul(user_input, client_socket, client_token)
        elif user_input[0:2] == 'dl':
            issue_dl(user_input, client_socket, client_token)
        elif user_input == 'Exit':
            message = ''.join(user_input + client_token)
            print('client sent this message: ', message)
            client_socket.sendall(message.encode())
            return False

    print('Exiting the application.')


if __name__ == '__main__':
    main()

