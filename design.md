
Goals:

- Do not write anything unvalidated into the database

How does the design look like:


- We request header batches of 4000
- Request [X - 4000, X]
- Each header consists of auth root and tree roots.
- We can verify tree roots for block X with auth root of block X and history tree of block X + 1 
- As a result, we request in batches of [X - 4000, X], [X, X + 4000], [X + 4000, X + 8000], etc.
   * See X, X + 4000,... are requested twice. We auth this block only on second batch arriving
- As a result, we maintain a separate CF for validated note commitment tree heights
  * TODO: is this necessary?

Implementation details:
- In the network layer, we maintain History tree cache as the header sync can run way ahead of the block body sync.