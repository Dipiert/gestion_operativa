import requests
import concurrent.futures
import logging as log
from time import perf_counter
from contextlib import contextmanager
import csv

log.basicConfig(
    format="{asctime}.{msecs:0<3.0f} {levelname} {message}",
    datefmt="%Y-%m-%d %I:%M:%S",
    style="{"
)
log.getLogger().setLevel(log.INFO)

base_url = 'https://6f008c57-99e0-4a2e-8d80-782a71cf99db.mock.pstmn.io/'


@contextmanager
def timer():
    start = perf_counter()
    yield lambda: perf_counter() - start


def get_entity_by_id(id, entity='orders'):
    with timer() as t:
        url = f'{base_url}/{entity}/{id}'
        res = requests.get(url)
        returned_id = res.json()['id']
        log.info(f'Call to {url} for an entity {entity}  with id {id} took {t():.4f} secs')
        if id != returned_id:
            log.warning(f"Discarding response: requested an entity {entity} with {id} but got {returned_id} instead")
            return None
        return res


def get_order(order_id):
    res = None
    with timer() as t:
        url = f'{base_url}/orders/{order_id}'
        res = requests.get(url)
        returned_id = res.json()['id']
        if order_id != returned_id:
            log.error(f"Discarding response: requested order {order_id} but got {returned_id} instead")
            return None
    log.info(f'Call to {url} for order_id {order_id} took {t():.4f} secs')
    return res


def get_shipment(shipment_id):
    with timer() as t:
        url = f'{base_url}/shipments/{shipment_id}'
        res = requests.get(url)
        returned_id = res.json()['id']
        log.info(f'Call to {url} for shipment_id {shipment_id} took {t():.4f} secs')
        if shipment_id != returned_id:
            log.warning(f"Discarding response: requested shipment {shipment_id} but got {returned_id} instead")
            return None
        return res


def _build_description(item):
    variation_attributes = item.get('variation_attributes')
    variations = [f"{var_att.get('name')}: {var_att.get('value_name')}" for var_att in variation_attributes]
    return "".join(variations)


def write_csv(orders, shipments):
    header = [
        'ID Orden', 'ID Item', 'Descripción producto', 'ID Envio', 'Estado',
        'Subestado', 'Tipo de logística', 'Destino de envío',
    ]
    for order in orders:
        order_items = order.get('order_items')
        for item in order_items:
            item = item.get('item')
            row = {
                'ID Order': order.get('id'),
                'ID Item': item.get('id'),
                'Descripción producto': _build_description(item),
            }


def main():
    MAX_THREADS = 1
    order_and_shipment = dict()
    #order_ids = [4114988927, 4114988960, 4114999549]
    order_ids = [4114988960]
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        orders = executor.map(get_order, order_ids)
        shipments = executor.map(get_shipment, order_ids)

    orders = [o.json() for o in orders if o]
    shipments = [s.json() for s in shipments if s]
    write_csv(orders, shipments)

    print()


if __name__ == '__main__':
    main()




