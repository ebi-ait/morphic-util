# common functions

import uuid
import pickle


def gen_uuid():
    return str(uuid.uuid4())


def is_valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False


def is_valid_dir_name(dir_name):
    if len(dir_name) < 36:
        return False
    elif len(dir_name) == 36: # without project name suffix
        return is_valid_uuid(dir_name)
    else:
        uuid_part = dir_name[0:36]
        if not is_valid_uuid(uuid_part):
            return False
        project_name = dir_name[36:] # format: -[alphanum]
        head = project_name[0]
        tail = project_name[1:]
        if head != '-':
            return False
        else:
            if tail.isalnum() and 0 < len(tail) < 13:
                return True
            else:
                return False


def serialize(name, obj):
    """Returns True if serialized."""
    try:
        pickle_out = open(name, 'wb')
        pickle.dump(obj, pickle_out)
        pickle_out.close()
        return True
    except Exception as e:
        print(str(e))
        return False


def deserialize(name):
    """Returns None if can't deserialize or not found."""
    try:
        pickle_in = open(name, 'rb')
        obj = pickle.load(pickle_in)
        pickle_in.close()
    except (OSError, IOError) as e:
        obj = None
    return obj
