from modelvalidation import Rest
from pydantic import ValidationError

def parse_json(raw_json):

    merchant = raw_json.get("merchant")

    if not merchant:
        return None
    tips_val = merchant.get("sofConfiguration", {}).get("tips")
    restaurant_details = {

        "Restaurant_Name": merchant.get("name"),

        "Restaurant_ID": merchant.get("ID"),

        "Branch_Name": merchant.get("branchName"),

        "Cuisine": merchant.get("cuisine"),

        "Tip": [tips_val] if tips val else [],

        "Timezone": merchant.get("timeZone"),

        "ETA": merchant.get("ETA"),

        "DeliveryOptions": merchant.get("deliveryOptions", []),

        "Rating": merchant.get("rating"),

        "Is_Open": bool(merchant.get("openingHours", {}).get("open")),

        "Currency_Code": merchant.get("currency", {}).get("code"),

        "Currency_Symbol": merchant.get("currency", {}).get("symbol"),

        "Offers": [],

        "Timing_Everyday": merchant.get("openingHours", {}).get("displayedHours")
    }
     for offers in merchant.get("offerCarousel", {}).get("offerHighlights", []):
        off = {
            "Title": offers.get("highlight").get("title"),
            "SubTitle": offers.get("highlight").get("subtitle")
        }
        restaurant_details["Offers"].append(off)

    categories = merchant.get("menu", {}).get("categories", [])

    menu_items = []

    for category in categories:

        category_name = category.get("name")

        for item in category.get("items", []):

            menu_items.append({

                "Restaurant_ID": merchant.get("ID"),

                "Category_Name": category_name,

                "Item_ID": item.get("ID"),

                "Item_Name": item.get("name"),

                "Item_Description": item.get("description"),

                "Item_Price": float(item.get("priceInMinorUnit", 0))/100,

                "Item_Discounted_Price":
                float(item.get("discountedPriceInMin", 0))/100,

                "Item_Image_URL": item.get("images") or [],

                "Item_Thumbnail_URL": item.get("thumbImages") or [],

                "Item_Available": bool(item.get("available")),

                "Is_Top_Seller": bool(item.get("topSeller"))
            })

    Rest_Data = {

        "Restaurant_Details": restaurant_details,

        "Menu_Items": menu_items
    }

  try:
        Rest(**Rest_Data)

        return Rest_Data

    except ValidationError as e:
        print("Error:",e)


