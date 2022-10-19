import os.path
import requests
import concurrent.futures
import logging as log
from time import perf_counter
from contextlib import contextmanager
import csv
from typing import Dict, Any, List

log.basicConfig(
    format="{asctime}.{msecs:0<3.0f} {levelname} {message}",
    datefmt="%Y-%m-%d %I:%M:%S",
    style="{"
)
log.getLogger().setLevel(log.INFO)

base_url = 'https://6f008c57-99e0-4a2e-8d80-782a71cf99db.mock.pstmn.io'


@contextmanager
def timer() -> float:
    start = perf_counter()
    yield lambda: perf_counter() - start


def get_order(order_id: int) -> requests.models.Response:
    """" Retrieve an order entity from its order_id """
    with timer() as t:
        url = f'{base_url}/orders/{order_id}'
        res = requests.get(url)
        returned_id = res.json()['id']
        if order_id != returned_id:
            log.error(f"Discarding response: requested order {order_id} but got {returned_id} instead")
            return None
        log.info(f'Retrieving data from {url} for order_id {order_id} took {t():.4f} secs')
        return res


def get_shipment(shipment_id: int) -> requests.models.Response:
    """" Retrieve a shipment entity from its shipment_id """
    with timer() as t:
        url = f'{base_url}/shipments/{shipment_id}'
        res = requests.get(url)
        returned_id = res.json()['order_id']
        if shipment_id != returned_id:
            log.warning(f"Discarding response: requested shipment {shipment_id} but got {returned_id} instead")
            return None
        log.info(f'Retrieving data from {url} for shipment_id {shipment_id} took {t():.4f} secs')
        return res


def _build_description(item: Dict[str, Any]) -> str:
    """" It uses variation_attributes to build a string like name: value_name """
    variation_attributes = item.get('variation_attributes')
    variations = [f"{var_att.get('name')}: {var_att.get('value_name')}" for var_att in variation_attributes]
    return "".join(variations)


def _build_shipment_destination(shipment: Dict[str, Any]) -> str:
    """" Returns agency and carrier id for Agencies. Full address for individuals  """
    receiver_address = shipment.get('receiver_address', {})
    is_agency = receiver_address.get('agency')
    if is_agency:
        agency_id = receiver_address.get('agency', {}).get('agency_id')
        carrier_id = receiver_address.get('agency', {}).get('carrier_id')
        return f'{agency_id} {carrier_id}'
    else:
        address_line = receiver_address.get('address_line', '')
        city_name = receiver_address.get('city', {}).get('name', '')
        zip_code = receiver_address.get('zip_code', '')
        return f'{address_line} {city_name} {zip_code}'


def write_csv(records: List[Dict], filename: str = 'condiciones_negocio_orders.csv') -> None:
    """ Write records into a filename provided by parameter. It checks that file doesn't exist """
    if not records:
        log.warning('Nothing to write into CSV file')
        return None
    log.info("Writing records into a CSV file")
    header = records[0].keys()

    if os.path.exists(filename):
        raise Exception(f'File {filename} already exists')

    with open(filename, 'w', newline='') as f:
        csv_writer = csv.DictWriter(f, header)
        csv_writer.writeheader()
        csv_writer.writerows(records)
        log.info('Wrote %s rows in %s', len(records), filename)


def build_order_rows(orders: List[Dict], shipments: List[Dict]) -> List:
    """" Builds a list of dicts merging orders and shipments """
    rows = []
    log.info("Merging orders and shipment data")
    for order in orders:
        order_items = order.get('order_items')
        for item in order_items:
            item = item.get('item', {})
            order_id = order.get('id')
            shipment = next((s for s in shipments if s['order_id'] == order_id), {})
            row = {
                'ID Order': order_id,
                'ID Item': item.get('id'),
                'Descripcion producto': _build_description(item),
                'ID Envio': shipment.get('order_id'),
                'Estado': shipment.get('status'),
                'Subestado': shipment.get('substatus'),
                'Tipo de logistica': shipment.get('logistic_type'),
                'Destino del envio': 'Domicilio' if shipment.get('receiver_address', {}).get('agency') else 'Agencia',
                'Direccion Receptor': _build_shipment_destination(shipment),
            }
            rows.append(row)
    return rows


def main() -> None:
    MAX_THREADS = 8
    order_ids = [4114988927, 4114988960, 4114999549]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        log.info('Getting orders and shipments asynchronously')
        orders = executor.map(get_order, order_ids)
        shipments = executor.map(get_shipment, order_ids)

    orders = [o.json() for o in orders if o]
    shipments = [s.json() for s in shipments if s]

    records = build_order_rows(orders, shipments)
    write_csv(records)
    log.info('Done')


if __name__ == '__main__':
    main()




