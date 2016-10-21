# Fast Collections for .NET

I'm putting together a bunch of useful collections for high-performance scenarios.

The first is a port of the Google B-tree library to C#.  It is written using unsafe code, and benchmarks within 10% or so of the original C++ version.  As of now, it only supports keys and values that don't contain managed object references.  This allows a very fast, memory-efficient solution.

I'll be adding more collections in the coming weeks.