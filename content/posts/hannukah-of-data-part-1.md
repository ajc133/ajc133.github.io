---
title: "Hannukah of Data: Part 1"
date: 2024-11-03T16:25:04-08:00
description: Solving where Noah's Rug has gone
categories: []
tags: []
---

*[Hannukah of Data: Noah's Rug](https://hanukkah.bluebird.sh/5784/)* is a super fun data puzzle where you must sift through a database from Noah's Market, "a bustling mom-and-pop everything store in Manhattan", and find past customers who can help you find Noah's Rug.

I'm going to attempt the challenges with a mix of Python and SQL.
I'll try using [Polars](https://docs.pola.rs/), a new DataFrame library which seems more intuitive to me than Pandas.
I know I'll be bailing out and using SQL as the tasks get harder, though.

Part 0 is getting access to the database files, which I'll leave as an exercise to the reader.

I'll also be leaving most of the challenge description/story out of this, but I really recommend playing along because it's funny story that plays out.

# Part 1: The Investigator

The goal is to find the customer whose phone number is equivalent to their last name spelled out (when translated into 10-key phone number presses).
For those unfamiliar with spelling out words with phone numbers, "SAM" would be "726" and "NOAH" would be "6624".

{{< figure src="/retro-cell-phone-keypad.png" alt="10-key phone keyboard" >}}

First, we'll need a function that converts a string (last name) to a phone number. 
Then, we'll need to run that function on everyone's last name and see if it matches their phone number.

```python
import polars as pl
customers = pl.read_csv("data/noahs-customers.csv",try_parse_dates=True)
```


```python
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
        
def name_to_number(name: str) -> str:
    return "".join([letter_to_number(l) for l in name if l.isalpha()])

```


```python
# Solution
last_names_translated = customers.select(
    pl.col("name")
    .str.extract(r"(.+) (.+)", group_index=2)
    .map_elements(name_to_number, return_dtype=pl.String)
    .alias("name_number"),
    pl.col("name"),
    pl.col("phone"),
)

print(last_names_translated.filter(
    pl.col("name_number") == pl.col("phone").str.replace_all("-", "")
))
```

    shape: (1, 3)
    ┌─────────────┬────────────────┬──────────────┐
    │ name_number ┆ name           ┆ phone        │
    │ ---         ┆ ---            ┆ ---          │
    │ str         ┆ str            ┆ str          │
    ╞═════════════╪════════════════╪══════════════╡
    │ 8266362286  ┆ Sam Tannenbaum ┆ 826-636-2286 │
    └─────────────┴────────────────┴──────────────┘


The solution code runs the `name_to_number` function on every customer's last name and compares that to their phone number.
Luckily there was only one person who came up: Sam Tannenbaum!
