#%%
import hashlib


def get_hash(file: str, buffer_size: int = 65536) -> str:
    """ Retorna o hash sha256 do arquivo """

    sha256=hashlib.sha256()
    with open(file, 'rb') as f:
        while True:
            data = f.read(buffer_size)
            if not data:
                break
        sha256.update(data)
    return(sha256.hexdigest())

