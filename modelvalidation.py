from pydantic import BaseModel
from typing import List, Optional


class Offer(BaseModel):

    Title: Optional[str]

    SubTitle: Optional[str]


class RestaurantDetails(BaseModel):

    Restaurant_Name: Optional[str]

    Restaurant_ID: Optional[str]

    Branch_Name: Optional[str]

    Cuisine: Optional[str]

    Tip: List[str] =[]

    Timezone: Optional[str]

    ETA: Optional[int]

    DeliveryOptions: List[str]

    Rating: Optional[float]

    Is_Open: Optional[bool]

    Currency_Code: Optional[str]

    Currency_Symbol: Optional[str]

    Offers: List[Offer]=[]

    Timing_Everyday: Optional[str]


class MenuItem(BaseModel):

    Restaurant_ID: Optional[str]

    Category_Name: Optional[str]

    Item_ID: Optional[str]

    Item_Name: Optional[str]

    Item_Description: Optional[str]

    Item_Price: Optional[float]

    Item_Discounted_Price: Optional[float]

    Item_Image_URL: Optional[list]

    Item_Thumbnail_URL: Optional[list]

    Item_Available: Optional[bool]

    Is_Top_Seller: Optional[bool]


class Rest(BaseModel):

    Restaurant_Details: RestaurantDetails


    Menu_Items: List[MenuItem]
