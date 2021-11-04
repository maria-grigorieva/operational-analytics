import configparser
from queues.tasks import occupancy

if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read('config.ini')

    result = occupancy.apply_async((config['PanDA DB']['connection_string'],))

    # at this time, our task is not finished, so it will return False
    print(result.ready())
    print(result.result)
    print(result.status)
