from pydantic import BaseModel, Field
from typing import List, Optional


class Rest(BaseModel):

    Restaurant_Name: Optional[str]
    Restaurant_ID: Optional[str]
    Branch_Name: Optional[str]

    Cuisine: Optional[str]

    Tip: List = []

    Timezone: Optional[str]

    ETA: Optional[int]=None

    DeliveryOptions: List = []

    Rating: Optional[float]

    Is_Open: bool

    Currency_Code: Optional[str]

    Currency_Symbol: Optional[str]

    Offers: List = []

    Timing_Everyday: Optional[str]

    Menu: Optional[str]
