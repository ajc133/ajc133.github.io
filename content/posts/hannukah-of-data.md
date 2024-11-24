---
date: "2024-11-24T15:27:08-08:00"
title: Hannukah of Data
---

<!-- -   [Part 1: The -->
<!--     Investigator](#part-1-the-investigator) -->
<!-- -   [Part 2: The -->
<!--     Contractor](#part-2-the-contractor) -->
<!-- -   [Part 3: The -->
<!--     Neighbor](#part-3-the-neighbor) -->
<!-- -   [Part 4: The Early -->
<!--     Bird](#part-4-the-early-bird) -->
<!-- -   [Part 5: The Cat -->
<!--     Lady](#part-5-the-cat-lady) -->
<!-- -   [Part 6: The Bargain -->
<!--     Hunter](#part-6-the-bargain-hunter) -->
<!--     -   [But first, what's a subway? (Red -->
<!--         herring)](#but-first-whats-a-subway-red-herring) -->
<!--     -   [Items sold at a -->
<!--         loss](#items-sold-at-a-loss) -->
<!-- -   [Part 7: The Meet -->
<!--     Cute](#part-7-the-meet-cute) -->
<!-- -   [Part 8: The -->
<!--     Collector](#part-8-the-collector) -->
<!-- -   [Conclusion](#conclusion) -->

*[Hannukah of Data: Noah's Rug](https://hanukkah.bluebird.sh/5784/)* is
a super fun data puzzle where you must sift through a database from
Noah's Market, "a bustling mom-and-pop everything store in Manhattan",
and find past customers who can help you find Noah's Rug.

I decided to solve these puzzles with Python and
[Polars](https://docs.pola.rs/), a new DataFrame library which seems
more intuitive to me than Pandas.

I consider myself beginner-intermediate with SQL and general data
querying, so some of my solutions are long-winded where others' would be
more concise. I still solved them all though!

Part 0 is getting access to the database files, which I'll leave as an
exercise to the reader.

I'll also be leaving most of the challenge description/story out of
this, but I really recommend playing along because it's funny story that
plays out.

``` python
# Load data
import polars as pl

customers = pl.read_csv("data/noahs-customers.csv", try_parse_dates=True)
orders_items = pl.read_csv("data/noahs-orders_items.csv", try_parse_dates=True)
orders = pl.read_csv("data/noahs-orders.csv", try_parse_dates=True)
products = pl.read_csv("data/noahs-products.csv", try_parse_dates=True)
```

# Part 1: The Investigator

The goal is to find the customer whose phone number is equivalent to
their last name spelled out on a phone keypad.

First, I'll need a function that converts a string (last name) to a
phone number. For example, "SAM" would be "726" and "NOAH" would be
"6624".

Then, I'll need to run that function on everyone's last name and see if
it matches their phone number.

``` python
def letter_to_number(letter: str) -> str:
    if len(letter) > 1:
        raise ValueError("You can only pass one letter at a time")

    letter = letter.lower()
    if letter in "abc":
        return "2"
    elif letter in "def":
        return "3"
    elif letter in "ghi":
        return "4"
    elif letter in "jkl":
        return "5"
    elif letter in "mno":
        return "6"
    elif letter in "pqrs":
        return "7"
    elif letter in "tuv":
        return "8"
    elif letter in "wxyz":
        return "9"
    else:
        return ""


def name_to_number(name: str) -> str:
    return "".join([letter_to_number(l) for l in name if l.isalpha()])
```

``` python
# Solution
last_names_translated = customers.select(
    pl.col("name")
    .str.extract(r"(.+) (.+)", group_index=2)
    .map_elements(name_to_number, return_dtype=pl.String)
    .alias("name_number"),
    pl.col("name"),
    pl.col("phone"),
)

print(
    last_names_translated.filter(
        pl.col("name_number") == pl.col("phone").str.replace_all("-", "")
    )
)
```

    shape: (1, 3)
    ┌─────────────┬────────────────┬──────────────┐
    │ name_number ┆ name           ┆ phone        │
    │ ---         ┆ ---            ┆ ---          │
    │ str         ┆ str            ┆ str          │
    ╞═════════════╪════════════════╪══════════════╡
    │ 8266362286  ┆ Sam Tannenbaum ┆ 826-636-2286 │
    └─────────────┴────────────────┴──────────────┘

The above code selects the "name" column in the customers table, takes
the last name, and runs the `name_to_number` function function on it.
The second line then filters all the rows where the above "last name
translated to phone number" (here I called the new column `name_number`)
is equal to the the phone number. Luckily there was only one person!

# Part 2: The Contractor

Finding the phone number in Part 1 unlocks Part 2, where you need to
find a contractor's phone number.

> ... **they usually talked about the project over coffee and bagels at
> Noah's before handing off the item to be cleaned. The contractors
> would pick up the tab and expense it, along with their cleaning
> supplies.**
>
> "So this rug was apparently one of those special projects. The claim
> ticket said '2017 JP'. **'2017' is the year the item was brought in,
> and 'JP' is the initials of the contractor.**

First I'll find all the customers whose initials are JP. How many could
there be?

``` python
jp_customers = customers.filter(pl.col("name").str.contains("^J.+ P.+$"))
print(jp_customers.height)
```

    72

How many orders did they collectively place in 2017?

``` python
# Get all of the orders (including items) from JP in 2017
orders_2017 = orders.filter(pl.col("ordered").dt.year() == 2017).join(
    jp_customers, on="customerid", how="inner"
)
orders_items_2017 = orders_items.join(orders_2017, on="orderid", how="inner")
print(len(orders_items_2017))
```

    486

Ok, so I need to find the customer who buys coffee, bagels, and cleaning
supplies. Visually inspecting the products table shows that I only need
to filter on products with descriptions containing "bagel", "coffee",
and "cleaner".

``` python
coffee_bagels_cleaners = products.filter(
    pl.col("desc").str.contains("Bagel|Coffee|Cleaner")
)
print(coffee_bagels_cleaners)
```

    shape: (4, 4)
    ┌─────────┬───────────────┬────────────────┬───────────────┐
    │ sku     ┆ desc          ┆ wholesale_cost ┆ dims_cm       │
    │ ---     ┆ ---           ┆ ---            ┆ ---           │
    │ str     ┆ str           ┆ f64            ┆ str           │
    ╞═════════╪═══════════════╪════════════════╪═══════════════╡
    │ BKY1573 ┆ Sesame Bagel  ┆ 1.02           ┆ 11.9|4.7|0.9  │
    │ HOM2761 ┆ Rug Cleaner   ┆ 1.43           ┆ 19.6|11.7|0.2 │
    │ BKY5717 ┆ Caraway Bagel ┆ 1.03           ┆ 11.3|2.3|1.6  │
    │ DLI8820 ┆ Coffee, Drip  ┆ 1.44           ┆ 9.6|7.8|0.7   │
    └─────────┴───────────────┴────────────────┴───────────────┘

I want all the order ids where coffee **AND** bagels **AND** cleaner
were purchased. I'll take the orders, collect all the SKUs in the order
into a list, and only keep the rows with lists that have three unique
SKUs.

``` python
potential_contractor_orders = (
    orders_items_2017.join(coffee_bagels_cleaners, on="sku", how="inner")
    .group_by("orderid")
    .agg(pl.col("sku"))
)

potential_contractor_orders_three_or_more = potential_contractor_orders.filter(
    pl.col("sku").list.n_unique() >= 3
)
print(potential_contractor_orders_three_or_more)
```

    shape: (1, 2)
    ┌─────────┬─────────────────────────────────┐
    │ orderid ┆ sku                             │
    │ ---     ┆ ---                             │
    │ i64     ┆ list[str]                       │
    ╞═════════╪═════════════════════════════════╡
    │ 7459    ┆ ["DLI8820", "HOM2761", "BKY157… │
    └─────────┴─────────────────────────────────┘

Apparently there's only one person with initals JP who bought those
items in one order in 2017.

``` python
print(
    potential_contractor_orders_three_or_more.join(
        orders_2017, on="orderid", how="inner"
    )
    .join(customers, on="customerid", how="inner")
    .select("customerid", "name", "phone")
)
```

    shape: (1, 3)
    ┌────────────┬─────────────────┬──────────────┐
    │ customerid ┆ name            ┆ phone        │
    │ ---        ┆ ---             ┆ ---          │
    │ i64        ┆ str             ┆ str          │
    ╞════════════╪═════════════════╪══════════════╡
    │ 1475       ┆ Joshua Peterson ┆ 332-274-4185 │
    └────────────┴─────────────────┴──────────────┘

# Part 3: The Neighbor

The contractor passed the rug along to a neighbor. Here's the important
info for finding them:

> I gave it to this guy who lived in my neighborhood... he was a Cancer
> born in the year of the Rabbit...

So I'll need to find the customer who: - lives in Joshua Peterson's
neighborhood - is a Cancer: born between the dates of June 21st and July
22nd ([source](https://www.horoscope.com/zodiac-signs/cancer)) - was
born in the year of the Rabbit: any of the following years: 2023, 2011,
1999, 1987, 1975, 1963, and 1951
([source](https://chinesenewyear.net/zodiac/rabbit/))

``` python
import datetime as dt

# Phone number is from part 2
joshua_peterson_citystatezip = (
    customers.filter(pl.col("phone") == "332-274-4185").select("citystatezip").item()
)
neighbors = customers.filter(pl.col("citystatezip") == joshua_peterson_citystatezip)

june_21_through_31 = (
    pl.col("birthdate").dt.month() == 6,
    pl.col("birthdate").dt.day().is_in(list(range(21, 32))),
)
july_1_through_22 = (
    pl.col("birthdate").dt.month() == 7,
    pl.col("birthdate").dt.day().is_in(list(range(1, 23))),
)

# I'd love to learn out how to OR these two filters together rather than concat
neighbors_june_cancer = neighbors.filter(june_21_through_31)
neighbors_july_cancer = neighbors.filter(july_1_through_22)

neighbors_cancer = pl.concat(
    [neighbors_june_cancer, neighbors_july_cancer], how="vertical"
)
print(neighbors_cancer.select("customerid", "name", "phone"))
```

    shape: (2, 3)
    ┌────────────┬───────────────┬──────────────┐
    │ customerid ┆ name          ┆ phone        │
    │ ---        ┆ ---           ┆ ---          │
    │ i64        ┆ str           ┆ str          │
    ╞════════════╪═══════════════╪══════════════╡
    │ 2550       ┆ Robert Morton ┆ 917-288-9635 │
    │ 9120       ┆ Max Moore     ┆ 315-291-6374 │
    └────────────┴───────────────┴──────────────┘

Wow, that narrows it down to two neighbors who are Cancers! At this
point I could manually inspect their birthdate, but I can just as easily
query it.

``` python
years_of_the_rabbit = [2023, 2011, 1999, 1987, 1975, 1963, 1951]
print(
    neighbors_cancer.filter(
        pl.col("birthdate").dt.year().is_in(years_of_the_rabbit)
    ).select("customerid", "name", "phone")
)
```

    shape: (1, 3)
    ┌────────────┬───────────────┬──────────────┐
    │ customerid ┆ name          ┆ phone        │
    │ ---        ┆ ---           ┆ ---          │
    │ i64        ┆ str           ┆ str          │
    ╞════════════╪═══════════════╪══════════════╡
    │ 2550       ┆ Robert Morton ┆ 917-288-9635 │
    └────────────┴───────────────┴──────────────┘

# Part 4: The Early Bird

The neighbor passed the rug along to a woman he met on Tinder.

> Thankfully, this woman I met on Tinder came over at **5am** with her
> bike chain repair kit and some pastries from Noah's. Apparently she
> liked to get up before dawn and **claim the first pastries that came
> out of the oven**.

So I need to find the customer who most often is the first to order
pastries before 5am.

It seems that the sku in the "products" table corresponds the department
of the store it's from. In this case, bakery items start with 'BKY'.

``` python
bakery_items = products.filter(pl.col("sku").str.starts_with("BKY"))
print(bakery_items)
```

    shape: (20, 4)
    ┌─────────┬─────────────────────────┬────────────────┬────────────────┐
    │ sku     ┆ desc                    ┆ wholesale_cost ┆ dims_cm        │
    │ ---     ┆ ---                     ┆ ---            ┆ ---            │
    │ str     ┆ str                     ┆ f64            ┆ str            │
    ╞═════════╪═════════════════════════╪════════════════╪════════════════╡
    │ BKY1573 ┆ Sesame Bagel            ┆ 1.02           ┆ 11.9|4.7|0.9   │
    │ BKY1679 ┆ Sesame Twist            ┆ 1.02           ┆ 6.9|6.2|1.6    │
    │ BKY2114 ┆ Poppyseed Mandelbrot    ┆ 1.05           ┆ 13.4|0.7|0.4   │
    │ BKY2596 ┆ Poppyseed Linzer Cookie ┆ 1.04           ┆ 16.0|7.5|5.0   │
    │ BKY4004 ┆ Raspberry Sufganiah     ┆ 1.02           ┆ 18.7|10.6|3.3  │
    │ …       ┆ …                       ┆ …              ┆ …              │
    │ BKY7535 ┆ Poppyseed Sufganiah     ┆ 0.85           ┆ 16.7|11.1|3.4  │
    │ BKY7716 ┆ Caraway Twist           ┆ 1.12           ┆ 12.9|9.4|6.7   │
    │ BKY8544 ┆ Raspberry Babka         ┆ 0.88           ┆ 15.4|14.1|14.0 │
    │ BKY8935 ┆ Caraway Bialy           ┆ 0.94           ┆ 15.4|14.5|1.2  │
    │ BKY9551 ┆ Sesame Puff             ┆ 1.03           ┆ 5.0|4.2|2.3    │
    └─────────┴─────────────────────────┴────────────────┴────────────────┘

Now I'll find the customer who places bakery orders before 5am the most
often.

``` python
bakery_items_orders = orders_items.join(bakery_items, on="sku", how="inner")
bakery_orders = orders.join(bakery_items_orders, on="orderid", how="inner")

early_bird_customer_id = (
    bakery_orders.filter(pl.col("ordered").dt.hour() < 5)
    .group_by("customerid")
    .agg(pl.len())
    .top_k(1, by="len")
    .item(0, 0)
)

print(
    customers.filter(pl.col("customerid") == early_bird_customer_id).select(
        "customerid", "name", "phone"
    )
)
```

    shape: (1, 3)
    ┌────────────┬──────────────┬──────────────┐
    │ customerid ┆ name         ┆ phone        │
    │ ---        ┆ ---          ┆ ---          │
    │ i64        ┆ str          ┆ str          │
    ╞════════════╪══════════════╪══════════════╡
    │ 2749       ┆ Renee Harmon ┆ 607-231-3605 │
    └────────────┴──────────────┴──────────────┘

# Part 5: The Cat Lady

> a woman in Staten Island came to pick it up... She said "I only have
> ten or eleven cats, and anyway they are getting quite old now...

I visually inspected the "products" table and found the pet food
section. They specifically sell senior cat food!

The goal is: - find people in Staten Island - find which one buys the
most senior cat food

``` python
# Find the staten islanders
staten_islanders = customers.filter(
    pl.col("citystatezip").str.starts_with("Staten Island")
)

# Find the senior cat food items
senior_cat_food_products = products.filter(
    pl.col("desc").str.contains("Senior Cat Food")
)

# Find who has bought the most senior cat food among all customers
orders_items_with_cat_food = orders_items.join(
    senior_cat_food_products, on="sku", how="inner"
)
orders_with_cat_food = orders.join(
    orders_items_with_cat_food, on="orderid", how="inner"
)
cat_food_mvp = (
    orders_with_cat_food.group_by("customerid")
    .agg(pl.col("qty").sum())
    .sort("qty", descending=True)
    .limit(1)
    .select("customerid")
    .item()
)

print(
    customers.filter(pl.col("customerid") == cat_food_mvp).select(
        "customerid", "name", "phone"
    )
)
```

    shape: (1, 3)
    ┌────────────┬───────────────┬──────────────┐
    │ customerid ┆ name          ┆ phone        │
    │ ---        ┆ ---           ┆ ---          │
    │ i64        ┆ str           ┆ str          │
    ╞════════════╪═══════════════╪══════════════╡
    │ 3068       ┆ Nicole Wilson ┆ 631-507-6048 │
    └────────────┴───────────────┴──────────────┘

# Part 6: The Bargain Hunter

> She's always been very frugal, and she clips every coupon and shops
> every sale at Noah's Market. In fact I like to tease her that Noah
> actually loses money whenever she comes in the store.
>
> Once the subway fare increased, she stopped coming to visit me... I
> hope she remembers to invite me to the family reunion next year.

If the market really does lose money whenever she purchases something,
it means that items she's buying cost less per-unit than they do
wholesale.

In other words, the market is **selling items at a loss** to Nicole
Wilson's cousin.

## But first, what's a subway? (Red herring)

Can we use the information that her cousin stopped visiting her due to
subway fare increase? This confused me at first, because all of the
trains on Staten Island are aboveground. Aren't subways *below ground?*.

But after poking around online, it seems like the whole system is
referred to as "The Subway" citywide, regardless if many sections are
aboveground. To get technical, Staten Island Railway is "operated by the
New York City Transit Authority **Department of Subways**" and "the line
uses modified R44 and R211S **subway cars**" according to [its Wikipedia
page](https://en.wikipedia.org/wiki/Staten_Island_Railway).

One important piece of info is that Staten Island is *not* connected to
any other part of NYC by rail. So maybe I'll get lucky and find only one
cousin (someone with the same last name as the lady from part 5,
hopefully) who lives on Staten Island.

``` python
print(
    customers.filter(
        pl.col("citystatezip").str.starts_with("Staten Island"),
        pl.col("name").str.ends_with("Wilson"),
    ).select("name", "phone")
)
```

    shape: (4, 2)
    ┌─────────────────┬──────────────┐
    │ name            ┆ phone        │
    │ ---             ┆ ---          │
    │ str             ┆ str          │
    ╞═════════════════╪══════════════╡
    │ Nicole Wilson   ┆ 631-507-6048 │
    │ Michaela Wilson ┆ 315-859-7694 │
    │ Thomas Wilson   ┆ 914-910-1529 │
    │ Edwin Wilson    ┆ 516-767-0295 │
    └─────────────────┴──────────────┘

Alas, none of those people are the answer.
I guess it was just to highlight her cheapness.
It was still a fun detour, though.

I'll make a table where I compute the profit made on every order.

``` python
orders_items_including_profit = products.join(
    orders_items, on="sku", how="inner"
).with_columns(profit=pl.col("unit_price") - pl.col("wholesale_cost"))

profit_per_order = orders_items_including_profit.group_by("orderid").agg(
    pl.col("profit").sum()
)
print(profit_per_order)
```

    shape: (213_232, 2)
    ┌─────────┬────────┐
    │ orderid ┆ profit │
    │ ---     ┆ ---    │
    │ i64     ┆ f64    │
    ╞═════════╪════════╡
    │ 121807  ┆ 2.68   │
    │ 69163   ┆ 0.66   │
    │ 111851  ┆ 0.84   │
    │ 132326  ┆ 5.29   │
    │ 175836  ┆ 5.17   │
    │ …       ┆ …      │
    │ 3367    ┆ 0.86   │
    │ 179104  ┆ 4.35   │
    │ 138848  ┆ 3.85   │
    │ 166376  ┆ 0.62   │
    │ 162393  ┆ 0.47   │
    └─────────┴────────┘

Then I'll find the customer who has the lowest (since it's negative)
total profit summed across their orders.

``` python
# Create a table with orderid, customerid, and profit
orders_including_profits = orders.select("orderid", "customerid").join(
    profit_per_order, on="orderid", how="inner"
)

# Group by customer id and take the sum of the profit
print(
    orders_including_profits.group_by("customerid")
    .agg(pl.col("profit").sum())
    .sort("profit")
    .head()
)
```

    shape: (5, 2)
    ┌────────────┬────────┐
    │ customerid ┆ profit │
    │ ---        ┆ ---    │
    │ i64        ┆ f64    │
    ╞════════════╪════════╡
    │ 4167       ┆ -85.59 │
    │ 8286       ┆ -1.04  │
    │ 7676       ┆ -0.17  │
    │ 6309       ┆ -0.04  │
    │ 2908       ┆ -0.02  │
    └────────────┴────────┘

It seems that customer with ID 4167 has costed Noah's Market \$85.59
over the course of their career as a penny-pincher.

``` python
print(
    customers.filter(pl.col("customerid") == 4167).select("customerid", "name", "phone")
)
```

    shape: (1, 3)
    ┌────────────┬─────────────┬──────────────┐
    │ customerid ┆ name        ┆ phone        │
    │ ---        ┆ ---         ┆ ---          │
    │ i64        ┆ str         ┆ str          │
    ╞════════════╪═════════════╪══════════════╡
    │ 4167       ┆ Sherri Long ┆ 585-838-9161 │
    └────────────┴─────────────┴──────────────┘

# Part 7: The Meet Cute

> I turned around to see this cute guy holding an item I had bought. He
> said, 'I got the same thing!' We laughed about it and wound up
> swapping items because I wanted the color he got. I asked him to get
> some food with me and we spent the rest of the day together.

First, I want to find all the items with colors in their names. After
inspecting the products table, it seems that colors are written in
**lower-case** at the end of the product description in parentheses.
I'll separate the product color into its own column.

``` python
# Products with colors in the name always end in e.g. `... (blue)`, so use a regex that pulls that color name out
color_name_filter = r"(.+) \(([[:lower:]]+)\)"
colorful_products = products.filter(pl.col("desc").str.contains(color_name_filter))
products_with_colors_extracted = colorful_products.select(
    pl.col("sku"),
    pl.col("desc").str.extract(color_name_filter, group_index=1),
    pl.col("desc").str.extract(color_name_filter, group_index=2).alias("color"),
)
print(products_with_colors_extracted)
```

    shape: (84, 3)
    ┌─────────┬──────────────────────┬─────────┐
    │ sku     ┆ desc                 ┆ color   │
    │ ---     ┆ ---                  ┆ ---     │
    │ str     ┆ str                  ┆ str     │
    ╞═════════╪══════════════════════╪═════════╡
    │ COL0037 ┆ Noah's Jewelry       ┆ green   │
    │ COL0065 ┆ Noah's Jewelry       ┆ mauve   │
    │ COL0166 ┆ Noah's Action Figure ┆ blue    │
    │ COL0167 ┆ Noah's Bobblehead    ┆ blue    │
    │ COL0483 ┆ Noah's Action Figure ┆ mauve   │
    │ …       ┆ …                    ┆ …       │
    │ COL9349 ┆ Noah's Action Figure ┆ orange  │
    │ COL9420 ┆ Noah's Jewelry       ┆ amber   │
    │ COL9773 ┆ Noah's Poster        ┆ magenta │
    │ COL9819 ┆ Noah's Lunchbox      ┆ blue    │
    │ COL9948 ┆ Noah's Jewelry       ┆ magenta │
    └─────────┴──────────────────────┴─────────┘

Ok now I'll find all orders with items that contain "colorful products".

``` python
orders_with_colorful_products = orders_items.join(
    products_with_colors_extracted, on="sku", how="inner"
)
print(orders_with_colorful_products)
```

    shape: (28_012, 6)
    ┌─────────┬─────────┬─────┬────────────┬──────────────────────┬─────────┐
    │ orderid ┆ sku     ┆ qty ┆ unit_price ┆ desc                 ┆ color   │
    │ ---     ┆ ---     ┆ --- ┆ ---        ┆ ---                  ┆ ---     │
    │ i64     ┆ str     ┆ i64 ┆ f64        ┆ str                  ┆ str     │
    ╞═════════╪═════════╪═════╪════════════╪══════════════════════╪═════════╡
    │ 1014    ┆ COL4117 ┆ 1   ┆ 4.55       ┆ Noah's Poster        ┆ yellow  │
    │ 1015    ┆ COL8357 ┆ 1   ┆ 13.48      ┆ Noah's Lunchbox      ┆ mauve   │
    │ 1018    ┆ COL6388 ┆ 1   ┆ 3.72       ┆ Noah's Gift Box      ┆ magenta │
    │ 1040    ┆ COL7454 ┆ 1   ┆ 10.65      ┆ Noah's Jersey        ┆ mauve   │
    │ 1041    ┆ COL2141 ┆ 1   ┆ 5.87       ┆ Noah's Bobblehead    ┆ puce    │
    │ …       ┆ …       ┆ …   ┆ …          ┆ …                    ┆ …       │
    │ 214217  ┆ COL9349 ┆ 1   ┆ 18.31      ┆ Noah's Action Figure ┆ orange  │
    │ 214218  ┆ COL0837 ┆ 1   ┆ 4.97       ┆ Noah's Poster        ┆ mauve   │
    │ 214227  ┆ COL1263 ┆ 1   ┆ 21.15      ┆ Noah's Action Figure ┆ yellow  │
    │ 214231  ┆ COL3430 ┆ 1   ┆ 13.45      ┆ Noah's Lunchbox      ┆ green   │
    │ 214232  ┆ COL6388 ┆ 1   ┆ 4.25       ┆ Noah's Gift Box      ┆ magenta │
    └─────────┴─────────┴─────┴────────────┴──────────────────────┴─────────┘

And now I have to find consecutive orders (the orderids auto-increment
by one) which have the same item but a different color... This is pretty
hard for me conceptually, because I don't know how to compare rows only
if they're sequential.

Maybe I could do a group_by on "desc" (which no longer includes color),
collect the orderids into a list, and compare the consecutive orders in
those lists.

``` python
orders_by_colorful_products = (
    orders_with_colorful_products.group_by("desc")
    .agg(pl.col("orderid").sort())  # Must sort orders for pairwise filter later
    .sort("desc")
)
print(orders_by_colorful_products)
```

    shape: (7, 2)
    ┌──────────────────────┬────────────────────────┐
    │ desc                 ┆ orderid                │
    │ ---                  ┆ ---                    │
    │ str                  ┆ list[i64]              │
    ╞══════════════════════╪════════════════════════╡
    │ Noah's Action Figure ┆ [1073, 1147, … 214227] │
    │ Noah's Bobblehead    ┆ [1041, 1071, … 214150] │
    │ Noah's Gift Box      ┆ [1018, 1167, … 214232] │
    │ Noah's Jersey        ┆ [1040, 1066, … 214184] │
    │ Noah's Jewelry       ┆ [1053, 1086, … 214202] │
    │ Noah's Lunchbox      ┆ [1015, 1086, … 214231] │
    │ Noah's Poster        ┆ [1014, 1047, … 214218] │
    └──────────────────────┴────────────────────────┘

For each product's list of order ids above, I must find all the order
ids that are consecutive. This is called going pairwise through the
list, and luckily python `itertools` implements that for me.

``` python
from itertools import pairwise


def filter_consecutive_ids(series: pl.List(pl.Int64)) -> list[tuple[int, int]]:
    output = []
    for pair in pairwise(series):
        if pair[1] - pair[0] == 1:
            output.append(pair)
    return output


items_ordered_consecutively = orders_by_colorful_products.select(
    pl.col("desc"),
    pl.col("orderid")
    .map_elements(filter_consecutive_ids, return_dtype=pl.List(pl.List(pl.Int64)))
    .alias("consecutive_orderid"),
).explode("consecutive_orderid")
print(items_ordered_consecutively)
```

    shape: (550, 2)
    ┌──────────────────────┬─────────────────────┐
    │ desc                 ┆ consecutive_orderid │
    │ ---                  ┆ ---                 │
    │ str                  ┆ list[i64]           │
    ╞══════════════════════╪═════════════════════╡
    │ Noah's Action Figure ┆ [2220, 2221]        │
    │ Noah's Action Figure ┆ [4573, 4574]        │
    │ Noah's Action Figure ┆ [4946, 4947]        │
    │ Noah's Action Figure ┆ [11945, 11946]      │
    │ Noah's Action Figure ┆ [13250, 13251]      │
    │ …                    ┆ …                   │
    │ Noah's Poster        ┆ [201437, 201438]    │
    │ Noah's Poster        ┆ [202011, 202012]    │
    │ Noah's Poster        ┆ [204001, 204002]    │
    │ Noah's Poster        ┆ [205182, 205183]    │
    │ Noah's Poster        ┆ [210059, 210060]    │
    └──────────────────────┴─────────────────────┘

This is a table of all the consecutive orders where a specific item
(ignoring its color) was bought. I now need to filter for the rows where
the color changed between each order in the list.

``` python
def get_orderids_to_item_color(series: pl.List(pl.Int64)) -> pl.List(pl.String):
    # orders_with_colorful_products is a global variable :)
    return orders_with_colorful_products.filter(pl.col("orderid").is_in(series)).select(
        "color"
    )


# - Add a 'color' column that has the color of each item from the corresponding orderid list
# - Filter for rows where exactly one item was bought in each order (length 2)
# - Filter for rows where the color list has two unique elements
items_ordered_consecutively_with_color = (
    items_ordered_consecutively.select(
        pl.col("desc"),
        pl.col("consecutive_orderid"),
        pl.col("consecutive_orderid")
        .map_elements(get_orderids_to_item_color, return_dtype=pl.List(pl.String))
        .alias("color"),
    )
    .filter(pl.col("color").list.len() == 2)
    .filter(pl.col("color").list.n_unique() == 2)
)
print(items_ordered_consecutively_with_color)
```

    shape: (401, 3)
    ┌──────────────────────┬─────────────────────┬─────────────────────┐
    │ desc                 ┆ consecutive_orderid ┆ color               │
    │ ---                  ┆ ---                 ┆ ---                 │
    │ str                  ┆ list[i64]           ┆ list[str]           │
    ╞══════════════════════╪═════════════════════╪═════════════════════╡
    │ Noah's Action Figure ┆ [2220, 2221]        ┆ ["orange", "amber"] │
    │ Noah's Action Figure ┆ [4573, 4574]        ┆ ["white", "purple"] │
    │ Noah's Action Figure ┆ [4946, 4947]        ┆ ["amber", "purple"] │
    │ Noah's Action Figure ┆ [13250, 13251]      ┆ ["azure", "purple"] │
    │ Noah's Action Figure ┆ [13649, 13650]      ┆ ["yellow", "green"] │
    │ …                    ┆ …                   ┆ …                   │
    │ Noah's Poster        ┆ [189701, 189702]    ┆ ["blue", "white"]   │
    │ Noah's Poster        ┆ [197802, 197803]    ┆ ["red", "azure"]    │
    │ Noah's Poster        ┆ [201437, 201438]    ┆ ["azure", "red"]    │
    │ Noah's Poster        ┆ [204001, 204002]    ┆ ["green", "yellow"] │
    │ Noah's Poster        ┆ [210059, 210060]    ┆ ["purple", "azure"] │
    └──────────────────────┴─────────────────────┴─────────────────────┘

I have the above table with 401 orders that are consecutive and have the
same item in a different color.

I'll find the rows where either orderid belongs to Sherri Long, the
woman from part 6.

``` python
part6_customerid = 4167
part6_customer_orderids = (
    orders.filter(pl.col("customerid") == part6_customerid)
    .select("orderid")
    .to_series()
)

colorful_orders_from_part6_customer = items_ordered_consecutively_with_color.filter(
    pl.col("consecutive_orderid")
    .list.eval(pl.element().is_in(part6_customer_orderids))
    .list.any()
)
print(colorful_orders_from_part6_customer)
```

    shape: (1, 3)
    ┌───────────────┬─────────────────────┬─────────────────────┐
    │ desc          ┆ consecutive_orderid ┆ color               │
    │ ---           ┆ ---                 ┆ ---                 │
    │ str           ┆ list[i64]           ┆ list[str]           │
    ╞═══════════════╪═════════════════════╪═════════════════════╡
    │ Noah's Poster ┆ [70502, 70503]      ┆ ["orange", "azure"] │
    └───────────────┴─────────────────────┴─────────────────────┘

I'll programatically get the order id + customer id of the customer that
is not Sherri Long from the above list of orderids.

``` python
print(
    colorful_orders_from_part6_customer.explode("consecutive_orderid")
    .filter(pl.col("consecutive_orderid").is_in(part6_customer_orderids).not_())
    .join(orders, left_on="consecutive_orderid", right_on="orderid", how="inner")
    .join(customers, on="customerid", how="inner")
    .select("customerid", "name", "phone")
)
```

    shape: (1, 3)
    ┌────────────┬──────────────┬──────────────┐
    │ customerid ┆ name         ┆ phone        │
    │ ---        ┆ ---          ┆ ---          │
    │ i64        ┆ str          ┆ str          │
    ╞════════════╪══════════════╪══════════════╡
    │ 5783       ┆ Carlos Myers ┆ 838-335-7157 │
    └────────────┴──────────────┴──────────────┘

# Part 8: The Collector

> I gave it to my sister. She wound up getting a newer and more
> expensive carpet, so she gave it to an acquaintance of hers who
> collects all sorts of junk. Apparently he owns an entire set of Noah's
> collectibles!

Giving it to his sister seems like a red herring, and there's not much
info I can glean from that. So instead, I'll focus on the acquaintance.

First step is to figure out what the collectibles look like in the
database. I learn from visual inspection that collectibles have a `sku`
starting with "COL".

``` python
collectibles = products.filter(pl.col("sku").str.starts_with("COL"))
print(collectibles)
```

    shape: (85, 4)
    ┌─────────┬───────────────────────────────┬────────────────┬────────────────┐
    │ sku     ┆ desc                          ┆ wholesale_cost ┆ dims_cm        │
    │ ---     ┆ ---                           ┆ ---            ┆ ---            │
    │ str     ┆ str                           ┆ f64            ┆ str            │
    ╞═════════╪═══════════════════════════════╪════════════════╪════════════════╡
    │ COL0037 ┆ Noah's Jewelry (green)        ┆ 28.32          ┆ 17.4|11.2|5.7  │
    │ COL0041 ┆ Noah's Ark Model (HO Scale)   ┆ 2487.35        ┆ 7.2|4.3|0.4    │
    │ COL0065 ┆ Noah's Jewelry (mauve)        ┆ 33.52          ┆ 19.0|12.2|10.5 │
    │ COL0166 ┆ Noah's Action Figure (blue)   ┆ 13.98          ┆ 12.1|7.7|7.2   │
    │ COL0167 ┆ Noah's Bobblehead (blue)      ┆ 5.36           ┆ 8.9|5.6|0.6    │
    │ …       ┆ …                             ┆ …              ┆ …              │
    │ COL9349 ┆ Noah's Action Figure (orange) ┆ 15.47          ┆ 16.6|12.9|11.9 │
    │ COL9420 ┆ Noah's Jewelry (amber)        ┆ 30.01          ┆ 13.8|9.1|4.9   │
    │ COL9773 ┆ Noah's Poster (magenta)       ┆ 4.13           ┆ 7.2|4.9|1.9    │
    │ COL9819 ┆ Noah's Lunchbox (blue)        ┆ 9.87           ┆ 14.2|4.1|3.3   │
    │ COL9948 ┆ Noah's Jewelry (magenta)      ┆ 32.97          ┆ 18.0|17.5|6.2  │
    └─────────┴───────────────────────────────┴────────────────┴────────────────┘

I'll find all the orders that contain any collectibles.

``` python
orders_items_collectibles = orders_items.join(collectibles, on="sku", how="inner")
print(orders_items_collectibles)
```

    shape: (28_013, 7)
    ┌─────────┬─────────┬─────┬────────────┬─────────────────────────┬────────────────┬────────────────┐
    │ orderid ┆ sku     ┆ qty ┆ unit_price ┆ desc                    ┆ wholesale_cost ┆ dims_cm        │
    │ ---     ┆ ---     ┆ --- ┆ ---        ┆ ---                     ┆ ---            ┆ ---            │
    │ i64     ┆ str     ┆ i64 ┆ f64        ┆ str                     ┆ f64            ┆ str            │
    ╞═════════╪═════════╪═════╪════════════╪═════════════════════════╪════════════════╪════════════════╡
    │ 1014    ┆ COL4117 ┆ 1   ┆ 4.55       ┆ Noah's Poster (yellow)  ┆ 3.63           ┆ 19.7|12.0|6.3  │
    │ 1015    ┆ COL8357 ┆ 1   ┆ 13.48      ┆ Noah's Lunchbox (mauve) ┆ 9.21           ┆ 19.9|17.4|9.8  │
    │ 1018    ┆ COL6388 ┆ 1   ┆ 3.72       ┆ Noah's Gift Box         ┆ 3.28           ┆ 19.0|9.5|2.2   │
    │         ┆         ┆     ┆            ┆ (magenta)               ┆                ┆                │
    │ 1040    ┆ COL7454 ┆ 1   ┆ 10.65      ┆ Noah's Jersey (mauve)   ┆ 8.19           ┆ 19.1|18.2|3.5  │
    │ 1041    ┆ COL2141 ┆ 1   ┆ 5.87       ┆ Noah's Bobblehead       ┆ 4.85           ┆ 19.0|10.4|10.2 │
    │         ┆         ┆     ┆            ┆ (puce)                  ┆                ┆                │
    │ …       ┆ …       ┆ …   ┆ …          ┆ …                       ┆ …              ┆ …              │
    │ 214217  ┆ COL9349 ┆ 1   ┆ 18.31      ┆ Noah's Action Figure    ┆ 15.47          ┆ 16.6|12.9|11.9 │
    │         ┆         ┆     ┆            ┆ (orange)                ┆                ┆                │
    │ 214218  ┆ COL0837 ┆ 1   ┆ 4.97       ┆ Noah's Poster (mauve)   ┆ 3.86           ┆ 13.0|3.3|0.8   │
    │ 214227  ┆ COL1263 ┆ 1   ┆ 21.15      ┆ Noah's Action Figure    ┆ 16.5           ┆ 10.2|7.5|3.6   │
    │         ┆         ┆     ┆            ┆ (yellow)                ┆                ┆                │
    │ 214231  ┆ COL3430 ┆ 1   ┆ 13.45      ┆ Noah's Lunchbox (green) ┆ 10.22          ┆ 16.0|6.9|1.9   │
    │ 214232  ┆ COL6388 ┆ 1   ┆ 4.25       ┆ Noah's Gift Box         ┆ 3.28           ┆ 19.0|9.5|2.2   │
    │         ┆         ┆     ┆            ┆ (magenta)               ┆                ┆                │
    └─────────┴─────────┴─────┴────────────┴─────────────────────────┴────────────────┴────────────────┘

Now I'll get the corresponding customerid for each of those rows.

``` python
customers_who_bought_collectibles = orders_items_collectibles.join(
    orders, on="orderid", how="inner"
)
print(customers_who_bought_collectibles.select("customerid"))
```

    shape: (28_013, 1)
    ┌────────────┐
    │ customerid │
    │ ---        │
    │ i64        │
    ╞════════════╡
    │ 4716       │
    │ 3808       │
    │ 2645       │
    │ 2520       │
    │ 8385       │
    │ …          │
    │ 6222       │
    │ 5950       │
    │ 8352       │
    │ 3894       │
    │ 1368       │
    └────────────┘

Group by customerid and get a list of the collectibles they bought.

``` python
customers_bought_num_collectibles = (
    customers_who_bought_collectibles.group_by("customerid")
    .agg(pl.col("sku"))
    .with_columns(pl.col("sku").list.len().alias("num_collectibles"))
    .sort("num_collectibles", descending=True)
)
print(customers_bought_num_collectibles.head())
```

    shape: (5, 3)
    ┌────────────┬─────────────────────────────────┬──────────────────┐
    │ customerid ┆ sku                             ┆ num_collectibles │
    │ ---        ┆ ---                             ┆ ---              │
    │ i64        ┆ list[str]                       ┆ u32              │
    ╞════════════╪═════════════════════════════════╪══════════════════╡
    │ 3808       ┆ ["COL8357", "COL6858", … "COL2… ┆ 111              │
    │ 1787       ┆ ["COL6461", "COL1263", … "COL9… ┆ 37               │
    │ 6855       ┆ ["COL8354", "COL9011", … "COL5… ┆ 36               │
    │ 3580       ┆ ["COL4363", "COL5018", … "COL2… ┆ 36               │
    │ 8352       ┆ ["COL9948", "COL9349", … "COL1… ┆ 34               │
    └────────────┴─────────────────────────────────┴──────────────────┘

Customer with id 3808 has bought the most collectibles by far.

``` python
print(
    customers.filter(pl.col("customerid") == 3808).select("customerid", "name", "phone")
)
```

    shape: (1, 3)
    ┌────────────┬─────────────┬──────────────┐
    │ customerid ┆ name        ┆ phone        │
    │ ---        ┆ ---         ┆ ---          │
    │ i64        ┆ str         ┆ str          │
    ╞════════════╪═════════════╪══════════════╡
    │ 3808       ┆ James Smith ┆ 212-547-3518 │
    └────────────┴─────────────┴──────────────┘

# Conclusion

Once I finished these puzzles, there was a speedrun that was unlocked which had larger datasets and slightly different questions.
Luckily my solutions were correct enough to only require minor tweaking to finish the speedrun and I did it in 14 minutes.

I had a lot of fun figuring out these puzzles, and I definitely improved my data analysis skills.

Major props to the creators of this game, "the [devottys](https://www.bluebird.sh/)".
It was well made, required careful analysis of the data and even the puzzle wording itself.

They also included amazing ascii art which reveals itself to be Noah's Rug hung over a fully lit menorah at the end.

{{< figure src="/noahs-tapestry-speedrun-finished.png" alt="ASCII art of Noah's Rug, a beautiful tapestry that includes all kinds of animals and insects, hung over a fully lit menorah" >}}
