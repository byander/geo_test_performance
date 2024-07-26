from datetime import datetime, timedelta
import time


def info(msg: str = '') -> None:
    """Envia uma uma mensagem no terminal com o data e hora atual
    """
    msg = f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} - {msg}'
    print(msg, flush=True)


def info_finished(start_time: float = 0) -> None:
    """Envia uma uma mensagem no terminal com o data e hora atual
    """
    finished = time.perf_counter()
    total_time = timedelta(seconds=finished - start_time)
    total_time_format = (datetime.min + total_time).time().strftime(
        "%H:%M:%S.%f")[:-3]

    info(f'Finished in {total_time_format}')
