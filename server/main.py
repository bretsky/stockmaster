import pyodbc
import os
import hashlib
import base64
import datetime

server = os.getenv("db_url")
port = os.getenv("db_port")
database = os.getenv('db_name')
username = os.getenv('db_username') 
password = os.getenv('db_password')

cnxn = pyodbc.connect(driver='{SQL Server}', database=database, server=','.join((server, port)), user=username, password=password)

cursor = cnxn.cursor()


def select(table, columns, order_by, **conditions):
	if not conditions:
		query = "SELECT {} FROM {}".format(", ".join(columns), table)
		if order_by:
			query += " ORDER BY {}".format(", ".join([o for o in order_by]))
		cursor.execute(query)
	else:
		query = "SELECT {} FROM {} WHERE {}".format(", ".join(columns), table, " AND ".join([c + conditions[c][0] + '?'  for c in conditions]))
		if order_by:
			query += " ORDER BY {}".format(", ".join([o for o in order_by]))
		cursor.execute(query, [conditions[c][1] for c in conditions])

def select_all(table, columns, **conditions):
	select(table, columns, None, **conditions)
	rows = cursor.fetchall()
	return rows

def select_all_ordered(table, columns, order_by, **conditions):
	select(table, columns, order_by, **conditions)
	rows = cursor.fetchall()
	return rows

def select_one(table, columns, **conditions):
	select(table, columns, None, **conditions)
	row = cursor.fetchone()
	return row

def insert_row(table, **values):
	keys = values.keys()
	query = "INSERT INTO {} ({}) VALUES ({})".format(table, ", ".join(keys), ", ".join(['?' for k in keys]))
	cursor.execute(query, [values[key] for key in keys])
	cnxn.commit()

def update_row(table, row_id, **values):
	keys = values.keys()
	query = "UPDATE {} SET {} WHERE ID=?".format(table, ", ".join([k + '=?' for k in keys]))
	cursor.execute(query, [values[key] for key in keys] + [row_id])
	cnxn.commit()

def upsert_row(table, conditions, **values):
	row = select_one(table, 'ID', **conditions)
	if row:
		row = row[0]
		update_row(table, row[0], **values)
	else:
		insert_row(table, **values)

def delete_row(table, **conditions):
	query = "DELETE FROM {} WHERE {}".format(table, " AND ".join([c + conditions[c][0] + '?'  for c in conditions]))
	cursor.execute(query, [conditions[c][1] for c in conditions])
	cnxn.commit()

def make_new_user(name, initial_balance, email, password):
	hashed_password = make_hash(password, 255)
	insert_row("TradingUsers", UserName=name, Balance=initial_balance, Email=email, UserPassword=hashed_password)

def make_hash(password, length=255):
	salt = os.urandom(32)
	key = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000, dklen=length - 32)
	storage = key + salt
	return storage

def check_hash(password, hashed, length=255):
	salt = hashed[length - 32:]
	key = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000, dklen=length - 32)
	if key == hashed[:length - 32]:
		return True
	return False

def authenticate(email, password):
	hashed = select_one("TradingUsers", ["UserPassword"], Email=("=", email))[0]
	return check_hash(password, hashed, 255)

def make_buy_order(user, symbol, volume, price, expiry):
	balance = select_one("TradingUsers", ["Balance"], ID=user)
	if balance < volume * price:
		return "Insufficient balance"
	now = datetime.datetime.utcnow()
	for row in select_all_ordered("Sells", ["TradingUser", "Volume", "Price", "ID"], ["Price ASC", "OrderDate ASC"], Symbol=("=", symbol), Price=("<=", price), ExpiryDate=(">", now)):
		if row[1] > volume:
			make_position(user, symbol, volume, row[2], now)
			sell_position(row[0], symbol, volume, row[2])
			make_transaction(user, row[0], symbol, row[2], volume, now)
			update_row("Sells", row[3], Volume=row[1] - volume)
			change_balance(user, -volume * row[2])
			change_balance(row[0], volume * row[2])
			return "Success"
		elif row[1] == volume:
			make_position(user, symbol, volume, row[2], now)
			sell_position(row[0], symbol, volume, row[2])
			make_transaction(user, row[0], symbol, row[2], volume, now)
			delete_row("Sells", ID=row[3])
			change_balance(user, -volume * row[2])
			change_balance(row[0], volume * row[2])
			return "Success"
		else:
			volume -= row[1]
			make_position(user, symbol, row[1], row[2], now)
			sell_position(row[0], symbol, row[1], row[2])
			make_transaction(user, row[0], symbol, row[2], row[1], now)
			delete_row("Sells", ID=row[3])
			change_balance(user, -row[1] * row[2])
			change_balance(row[0], row[1] * row[2])
	insert_row("Buys", TradingUser=user, Symbol=symbol, Volume=volume, Price=price, OrderDate=now, ExpiryDate=expiry)
	return "Success"

def make_sell_order(user, symbol, volume, price, expiry):
	positions = select_all("Positions", ["Volume"], ID=user, Symbol=symbol)
	if sum(positions) < volume:
		return "Insufficient shares"
	now = datetime.datetime.utcnow()
	for row in select_all_ordered("Buys", ["TradingUser", "Volume", "Price", "ID"], ["Price DESC", "OrderDate ASC"], Symbol=("=", symbol), Price=(">=", price), ExpiryDate=(">", now)):
		if row[1] > volume:
			make_position(row[0], symbol, volume, row[2], now)
			sell_position(user, symbol, volume, row[2])
			make_transaction(row[0], user, symbol, row[2], volume, now)
			update_row("Buys", row[3], Volume=row[1] - volume)
			change_balance(user, volume * row[2])
			change_balance(row[0], -volume * row[2])
			return "Success"
		elif row[1] == volume:
			make_position(row[0], symbol, volume, row[2], now)
			sell_position(user, symbol, volume, row[2])
			make_transaction(row[0], user, symbol, row[2], volume, now)
			delete_row("Buys", ID=row[3])
			change_balance(user, volume * row[2])
			change_balance(row[0], -volume * row[2])
			return "Success"
		else:
			volume -= row[1]
			make_position(row[0], symbol, row[1], row[2], now)
			sell_position(user, symbol, row[1], row[2])
			make_transaction(row[0], user, symbol, row[2], row[1], now)
			delete_row("Buys", ID=row[3])
			change_balance(user, row[1] * row[2])
			change_balance(row[0], -row[1] * row[2])
	insert_row("Sells", TradingUser=user, Symbol=symbol, Volume=volume, Price=price, OrderDate=now, ExpiryDate=expiry)
	return "Success"

def get_all_columns(table):
	return select_all("INFORMATION_SCHEMA.COLUMNS", "*", TABLE_NAME=['=', table])

def sell_position(user, symbol, volume, sell_price):
	for row in select_all_ordered("Positions", ["Volume", "ID"], ["OpenPrice ASC"], Symbol=("=", symbol), TradingUser=("=", user)):
		if row[0] > volume:
			update_row("Positions", row[1], Volume=row[0] - volume)
			return
		elif row[0] == volume:
			delete_row("Positions", ID=row[1])
			return
		else:
			volume -= row[0]
			delete_row("Positions", ID=row[1])

def make_position(user, symbol, volume, open_price, open_date):
	insert_row("Positions", TradingUser=user, Symbol=symbol, Volume=volume, OpenPrice=open_price, OpenDate=open_date)

def make_transaction(buyer, seller, symbol, price, volume, date):
	insert_row("Transactions", Buyer=buyer, Seller=seller, Symbol=symbol, Price=price, Volume=volume, Date=date)

def change_balance(user, amount):
	current_balance = select_one("TradingUsers", ["Balance"], ID=user)[0]
	update_row("TradingUsers", user, Balance=current_balance + amount)

def get_user_id(user_email):
	return select_one("TradingUsers", ["ID"], Email=["=", user_email])[0]



print(select_all("Sells", ["*"]))
print(select_all("Buys", ["*"]))

print(select_all("Positions", ["*"]))
print(select_all("Transactions", ["*"]))


