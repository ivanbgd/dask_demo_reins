# Description

A major commercial insurance company (Company A) insures a number of companies against property damage from adverse events.
The companies and the type and location of the covered events are listed in "deals.csv".
To limit their exposure to potential losses, they decide to buy a reinsurance contract, covering them in case of certain large events.
The contract, described in "contract.json", covers all events in the USA and Canada, excluding tornados, up to a maximum amount of $3,000 on any one event.

Find the deals covered by the reinsurance contract, filtering "deals.csv" using the coverage details in "contract.json".

The output should be something like:

| DealId | Company |   Peril    | Location |
|:------:|:-------:|:----------:|:--------:|
|   1    | ClientA | Earthquake |   USA    |
|   2    | ClientA | Hailstone  |  Canada  |
|   5    | ClientC | Hurricane  |   USA    |

When modeling the risk on this reinsurance contract at Company B we run a
simulation to model expected losses to the underlying insurance deals. In the
scenario described in "losses.csv", where a number of events occur, how much could
Company A claim on the reinsurance contract?
Group the answer by the perils being insured.

The output should be something like:

|   Peril    | Loss |
|:----------:|:----:|
| Earthquake | 3500 |
| Hurricane  | 3000 |


# Implementation

This implementation is based on the Python [Dask](https://www.dask.org/) library for Big Data processing.

It can scale up to clusters of computers, but can be used on a single computer just as easily.
This means that it can employ multiple cores, as well as multiple processors
on a single computer, or in a cluster (distributed computing).

Dask also helps us overcome memory limitations.
With Dask, we can use data sets that don't fit in available RAM.
This means that if we have a huge input file, we won't have a problem. It will be read in blocks (chunks).
In conclusion, this solution will scale well, both CPU-wise and memory-wise.

File "requirements.txt" lists third-party libraries we have used for this project.
Those are the latest stable versions.

Additionally, a database could possibly help if it were used to store data from CSV files.
It makes sense if we are going to use the data at least several times.
This is because we first have to fill the DB with data from a CSV file.
In case we are processing data only once, we are probably better off without a DB.
We could also write results to a DB.

There is some input data validation.


# Unit Tests

To run the provided unit test suite, execute:

`python -m unittest`

For verbose output:

`python -m unittest -v`
