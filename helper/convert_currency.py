# convert inr to idr
def convert_to_idr(price, rate):
    # extract number from string
    price = price.replace('₹', '').replace(',', '')
    try:
        price_inr = float(price)
    except ValueError:
        return "Invalid Price"
    
    # convert to idr
    convert_price = price_inr * rate
    return f'{convert_price}'

# convert usd to idr
# def convert_from_usd(price, rate):
#     try:
#         price_usd = float(price)
#     except ValueError:
#         return "Invalid Price"
    
#     # convert to idr
#     convert_price = price_usd * rate
    
#     return convert_price