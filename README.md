# go-kelips
go-kelips implements the kelips DHT in golang.

It has a pluggable design with the following available interfaces:

- Transport
- ContactStorage
- TupleStorage

A gossip layer has also been included.  Its design is to augment the core kelips node
as to not be invasive and allow the gossip layer to be pluggable as well.