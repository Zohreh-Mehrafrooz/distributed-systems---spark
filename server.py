# done by Zohreh Mehrafrooz , Id: 40218210
import socket
import random
import string
from threading import Thread
import os
import shutil
from pathlib import Path


server_root_path = os.getcwd()


def get_working_directory_info(working_directory):
    """
    Creates a string representation of a working directory and its contents.
    :param working_directory: path to the directory
    :return: string of the directory and its contents.
    """
    dirs = '\n-- ' + '\n-- '.join([i.name for i in Path(working_directory).iterdir() if i.is_dir()])
    files = '\n-- ' + '\n-- '.join([i.name for i in Path(working_directory).iterdir() if i.is_file()])
    dir_info = f'Current Directory: {working_directory}:\n|{dirs}{files}'
    return dir_info


def generate_random_eof_token():
    """Helper method to generates a random token that starts with '<' and ends with '>'.
     The total length of the token (including '<' and '>') should be 10.
     Examples: '<1f56xc5d>', '<KfOVnVMV>'
     return: the generated token.
     """
    # initializing size of string
    n = 8

    # using random.choices() generating random strings
    res_random = ''.join(random.choices(string.ascii_lowercase + string.digits, k=n))
    res = ''.join(('<'+str(res_random)+'>'))

    # print result
    print("The generated random string : " + str(res))
    return str(res)


def receive_message_ending_with_token(active_socket, buffer_size, eof_token):
    """
    Same implementation as in receive_message_ending_with_token() in client.py
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


def handle_cd(current_working_directory, new_working_directory):
    """
    Handles the client cd commands. Reads the client command and changes the current_working_directory variable 
    accordingly. Returns the absolute path of the new current working directory.
    :param current_working_directory: string of current working directory
    :param new_working_directory: name of the sub directory or '..' for parent
    :return: absolute path of new current working directory
    """
    os.chdir(current_working_directory)
    os.chdir(new_working_directory)
    print(os.getcwd())
    if os.path.samefile(os.getcwd(), server_root_path):
        print('Access Denied')
        return current_working_directory
    else:
        return os.getcwd()


def handle_mkdir(current_working_directory, directory_name):
    """
    Handles the client mkdir commands. Creates a new sub directory with the given name in the current working directory.
    :param current_working_directory: string of current working directory
    :param directory_name: name of new sub directory
    """
    path = os.path.join(current_working_directory, directory_name)
    os.mkdir(path)


def handle_rm(current_working_directory, object_name):
    """
    Handles the client rm commands. Removes the given file or sub directory. Uses the appropriate removal method
    based on the object type (directory/file).
    :param current_working_directory: string of current working directory
    :param object_name: name of sub directory or file to remove
    """
    object_name_path = Path(''.join(current_working_directory+'/'+object_name))
    print(object_name_path)
    if os.path.isdir(object_name_path.as_posix()):
        os.rmdir(object_name_path)
    elif os.path.isfile(object_name_path.as_posix()):
        os.remove(object_name_path)
    else:
        print('error in remove file or directory')


def handle_ul(current_working_directory, file_name, service_socket, eof_token):
    """
    Handles the client ul commands. First, it reads the payload, i.e. file content from the client, then creates the
    file in the current working directory.
    Use the helper method: receive_message_ending_with_token() to receive the message from the client.
    :param current_working_directory: string of current working directory
    :param file_name: name of the file to be created.
    :param service_socket: active socket with the client to read the payload/contents from.
    :param eof_token: a token to indicate the end of the message.
    """
    file_content = receive_message_ending_with_token(service_socket, 1024, eof_token)

    os.chdir(current_working_directory)
    with open(file_name, 'wb') as f:
        f.write(file_content)


def handle_dl(current_working_directory, file_name, service_socket, eof_token):
    """
    Handles the client dl commands. First, it loads the given file as binary, then sends it to the client via the
    given socket.
    :param current_working_directory: string of current working directory
    :param file_name: name of the file to be sent to client
    :param service_socket: active service socket with the client
    :param eof_token: a token to indicate the end of the message.
    """
    """Using shutil
    server_directory_file = os.path.join(current_working_directory, file_name)
    print('server_directory_file', server_directory_file)
    client_directory_file = os.path.join(uploaded_downloaded_directory_client, file_name)
    print('client_directory_file', client_directory_file)

    read_file_server = open(server_directory_file, 'rb')
    write_file_client = open(client_directory_file, 'wb')

    shutil.copyfileobj(read_file_server, write_file_client)"""

    os.chdir(current_working_directory)
    with open(file_name, 'rb') as f:
        file_content = f.read()

    file_content_with_token = file_content + eof_token.encode()
    service_socket.sendall(file_content_with_token)


class ClientThread(Thread):

    def __init__(self, service_socket: socket.socket, address: str):
        Thread.__init__(self)
        self.service_socket = service_socket
        self.address = address
        self.cwd = server_root_path

    def run(self):
        print("Connection from : ", self.address)
        # initialize the connection
        # send random eof token
        eof_token = generate_random_eof_token()
        self.service_socket.sendall(eof_token.encode())
        print(f'Sent "{eof_token}" to: {self.address}')

        # establish working directory
        message_get_cwd = 'Enter the name of your directory'
        self.service_socket.sendall(message_get_cwd.encode())
        working_directory_user = self.service_socket.recv(1024)
        establish_working_directory = os.path.join(self.cwd, working_directory_user.decode())
        if os.path.isdir(establish_working_directory):
            working_directory = establish_working_directory
        else:
            os.mkdir(establish_working_directory)
            working_directory = establish_working_directory

        print('working_directory ', working_directory)

        # send the current dir info
        working_directory_info = ''.join((str(get_working_directory_info(establish_working_directory)) + str(eof_token)))
        self.service_socket.sendall(working_directory_info.encode())
        print(f'Sent "{working_directory_info}" to: {self.address}')

        # get the command and arguments and call the corresponding method
        # send current dir info
        while True:
            received_argument = receive_message_ending_with_token(self.service_socket, 1024, eof_token)
            print('received_argument', received_argument.decode())
            if received_argument.decode()[0:2] == 'cd':
                print('message is cd .. or directory name')
                working_directory = handle_cd(working_directory, received_argument.decode()[3:])
            elif received_argument.decode()[0:2] == 'mk':
                print('message is mkdir.')
                handle_mkdir(working_directory, received_argument.decode()[3:])
            elif received_argument.decode()[0:2] == 'rm':
                print('message is rmdir.')
                handle_rm(working_directory, received_argument.decode()[3:])
            elif received_argument.decode()[0:2] == 'ul':
                print('message is handle_ul.')
                handle_ul(working_directory, received_argument.decode()[3:], self.service_socket, eof_token)
            elif received_argument.decode()[0:2] == 'dl':
                print('message is handle_dl.')
                handle_dl(working_directory, received_argument.decode()[3:], self.service_socket, eof_token)
            elif received_argument.decode() == 'Exit':
                print('Connection closed from:', self.address)
                return False
            else:
                print('no valid command form client')

            working_directory_info = ''.join((str(get_working_directory_info(working_directory)) + str(eof_token)))
            self.service_socket.sendall(working_directory_info.encode())


def main():

    HOST = "127.0.0.1"
    PORT = 65432

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        # s.close()
        while True:
            conn, addr = s.accept()
            client_thread = ClientThread(conn, addr)
            print(client_thread.cwd)
            client_thread.start()


if __name__ == '__main__':
    main()


