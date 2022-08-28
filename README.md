# GraphQL

A tool similar to graphql. 

## Run examples with deno

```bash
deno run -A examples.ts
```

## Build with nodejs

```bash
npm install typescript -g
npm install uglify-js -g
tsc -t es2016 -d graphql.ts && uglifyjs graphql.js -m -o graphql.min.js
```

## Example

See [examples.ts](examples.ts)
