# Gestion Operativa

## Rationale
This script allows to visualize business conditions from an order.

## Features

* It uses multithreading to scale requests to hundreds of order requests.
* It writes a CSV file with the following info:
  * Order:
    * Order ID
    * Item ID
    * Product Description (including variations)
  * Shipment:
    * Shipment ID
    * Shipment Status and substatus
    * Logistic type
    * Shipment destination
    * Agency and carrier ID for agencies
    * Address for individuals

## Links

* https://developers.mercadolibre.com.ar/es_ar/mercadoenvios-modo-2
* https://developers.mercadolibre.com.ar/es_ar/gestiona-ventas

## Nice to have / To Do

* Allow parameterization by CLI and/or Configuration file of:
  * BASE_URL
  * Number of threads
  * Order IDs
* Persist logs into files and write errors in files: rotate those logs as per user params.
* Write unit tests :)
* Refactor get_order and get_shipment into a function like 'get_entity_by_id'