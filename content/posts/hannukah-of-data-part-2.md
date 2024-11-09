---
title: "Part 2: The Contractor"
date: 2024-11-09T09:11:48-08:00
tags: ["hannukah-of-data"]
---

Finding the phone number in [Part 1]({{< ref "hannukah-of-data-part-1" >}}) of the Hannukah of Data unlocks Part 2, where you need to find a contractor's phone number.

> ... **they usually talked about the project over coffee and bagels at Noah’s before handing off the item to be cleaned. The contractors would pick up the tab and expense it, along with their cleaning supplies.**
> 
> “So this rug was apparently one of those special projects. The claim ticket said ‘2017 JP’. **‘2017’ is the year the item was brought in, and ‘JP’ is the initials of the contractor.**



```python
import polars as pl

customers = pl.read_csv("data/noahs-customers.csv",try_parse_dates=True)
orders_items = pl.read_csv("data/noahs-orders_items.csv",try_parse_dates=True)
orders = pl.read_csv("data/noahs-orders.csv",try_parse_dates=True)
products = pl.read_csv("data/noahs-products.csv",try_parse_dates=True)
```

First I'll find all the customers whose initials are JP.


```python
jp_customers = customers.filter(pl.col("name").str.contains("^J.+ P.+$"))
print(len(jp_customers), "/", len(customers))
```

    72 / 8260


Out of the 8,260 customers, 72 have initials JP.
How many orders did they collectively place in 2017?


```python
# Get all of the orders (including items) from JP in 2017
orders_2017 = orders.filter(pl.col("ordered").dt.year() == 2017).join(
    jp_customers, on="customerid", how="inner"
)
orders_items_2017 = orders_items.join(orders_2017, on="orderid", how="inner")
print(len(orders_items_2017))
```

    486


Ok, so I need to find the customer who buys coffee, bagels, and cleaning supplies.
Visually inspecting the products table shows that I only need to filter on products with descriptions containing "bagel", "coffee", and "cleaner".


```python
coffee_bagels_cleaners = products.filter(pl.col("desc").str.contains("Bagel|Coffee|Cleaner"))
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


I want all the order ids where coffee **AND** bagels **AND** cleaner were purchased, but I'm going to take a shortcut and find the orders where coffee **OR** bagels **OR** cleaner were purchased.
Then I'll take the orders that had three items or more.


```python
# Get all of the orders that had coffee OR bagels OR cleaner, since I'm too lazy
# to solve how to do it with AND
potential_contractor_orders = (
    coffee_bagels_cleaners.join(orders_items_2017, on="sku", how="inner")
    .group_by("orderid")
    .agg(pl.len())
    .filter(pl.col("len") >= 3)
)
print(potential_contractor_orders)

```

    shape: (1, 2)
    ┌─────────┬─────┐
    │ orderid ┆ len │
    │ ---     ┆ --- │
    │ i64     ┆ u32 │
    ╞═════════╪═════╡
    │ 7459    ┆ 3   │
    └─────────┴─────┘


This happens to work in this case, but I suspect it'll fail in larger datasets. 
I find it hard to believe there's only one person with initals JP who bought three coffees in 2017.

Now I find the customer who placed that order.


```python
print(potential_contractor_orders.join(orders_2017, on="orderid", how="left").join(
    customers, on="customerid", how="inner"
).select("name", "phone"))

```

    shape: (1, 2)
    ┌─────────────────┬──────────────┐
    │ name            ┆ phone        │
    │ ---             ┆ ---          │
    │ str             ┆ str          │
    ╞═════════════════╪══════════════╡
    │ Joshua Peterson ┆ 332-274-4185 │
    └─────────────────┴──────────────┘

