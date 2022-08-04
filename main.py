import json
import os
from datetime import datetime, timedelta
import requests
from secret_manager import get_secret
from connection import db_connector


shopify_secret = os.environ['shopify_secret']
secret = get_secret(shopify_secret)
secret = json.loads(secret)

api_key = secret.get('api_key')
api_secret = secret.get("api_secret")
shop_url = secret.get('shop_url')
api_version = secret.get('api_version')


def call_api(url, method):
    response = None
    if method == "get":
        response = requests.get(url)
    print("status_code", response.status_code)
    return response


def create_order_details(data=None, table_name=""):
    conn = db_connector()
    curr = conn.cursor()
    last_row_id = None
    try:
        if isinstance(data, dict):
            insert_qry = 'INSERT INTO {0} SET {1} '.format(table_name,
                                                           ', '.join('{}=%s'.format(k) for k in data))
            if curr.execute(insert_qry, list(data.values())):
                conn.commit()
                if table_name == "order_details":
                    last_row_id = curr.lastrowid
    except Exception as err:
        print(str(err))
    conn.close()
    return last_row_id


def get_data_from_url():
    try:
        d = datetime.now() - timedelta(hours=0, minutes=10)
        shop_api = 'https://{0}:{1}@{2}.myshopify.com/admin'.format(api_key, api_secret, shop_url)
        endpoint = f'/api/{api_version}/orders.json?updated_at_min={d}'
        request_url = shop_api + endpoint
        response = call_api(url=request_url, method='get')
        if response:
            if response.status_code == 200:
                data = response.text
                data = json.loads(data)
                orders = data.get('orders', None)
                if orders:
                    if isinstance(orders, list):
                        for order in orders:
                            customer = order.get('customer', {})
                            order_data = {'app_id': order.get("app_id"), 'browser_ip': order.get("browser_ip"),
                                          'cancel_reason': order.get("cancel_reason"),
                                          'cancelled_at': order.get("cancelled_at"),
                                          'created_at': order.get("created_at"), "currency": order.get("currency"),
                                          "current_total_discounts": order.get("current_total_discounts"),
                                          "current_total_price": order.get("current_total_price"),
                                          "current_subtotal_price": order.get("current_subtotal_price"),
                                          "current_total_tax": order.get("current_total_tax"),
                                          "email": order.get("email"),
                                          "accepts_marketing": customer.get('accepts_marketing'),
                                          "first_name": customer.get('first_name'),
                                          "last_name": customer.get('last_name'),
                                          'state': customer.get("state"), 'note': customer.get("note"),
                                          'verified_email': customer.get("verified_email"),
                                          'multipass_identifier': customer.get("multipass_identifier"),
                                          'tax_exempt': customer.get("tax_exempt"), 'phone': customer.get("phone"),
                                          'tags': order.get("tags"),
                                          'gateway': order.get("gateway"),
                                          'fulfillment_status': order.get("fulfillment_status"),
                                          'location_id': order.get("location_id"),
                                          "total_weight": order.get("total_weight"),
                                          'total_tip_received': order.get("total_tip_received"),
                                          'updated_at': order.get("updated_at"), 'user_id': order.get("user_id")}
                            address_data = order.get('billing_address')
                            fk_id = create_order_details(order_data, 'order_details')
                            address_data['order_details_fk'] = fk_id
                            create_order_details(address_data, 'origin_address')
        print('data successfully insert')
    except Exception as err:
        print(str(err))
