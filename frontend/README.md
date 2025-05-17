# SPA kollektivkart

I have no idea what any of this does. I'm trying to learn some frontend! 🥳

I'm hoping this eventually can replace the dash app in the backend. Then we could experiment with implementing the backend in something else than Python entirely, like Rust or Scala! Wouldn't that be fun? But first, we should try to reach feature parity with the dash app and try to (re)learn some CSS 🎓

## Commands

Install dependencies

```shell
npm install
```

Run in development mode:

```shell
npm run dev
```

Type check:

```shell
npm run check
```

Lint:

```shell
npm run lint
```

## Core dependencies

- [plotly](https://plotly.com/javascript/)
- [react](https://react.dev) - maybe it would be fun to try using something else? 🧐

This puts a shockingly large 372MB in `node_modules` and the single javascript file we need is a 5MB download. Would be good to find something more minimal or configure the build to make this a smaller download!

## API

This uses an API that is currently mounted to `https://kollektivkart.kaveland.no/api/`, see [api.ts](./api.ts). The code for that is in [api.py](../kollektivkart/api.py). The API is running an in-memory DuckDB database and essentially sends tabular result sets directly as JSON, like this:

```json
{
  "name": ["first", "second"],
  "value": [0, 1]
}
```

This is a conscious decision to make the large result sets transfer faster, they're much faster to serialize/deserialize and transfer like this. The API is a very thin connection between DuckDB and HTTP. On the backend, it responds in usually 40ms-80ms, which is much faster than the dash app! (2x-3x)

We also allow caching, since data only changes nightly.

## Work

- Implement the hotspot map ✅
- Make the gnarly tooltip ✅
- Make the tooltip less gnarly
- Deployment! Should use bunny.net and [thumper](https://kaveland.no/thumper/) for this.
  + We probably want a super aggressive cache-policy for the index.html document, but let the index.js (the app) live essentially forever. I wonder how we can do that? 🧐
- Inject the URL for the API from the build so we can run both backend and frontend locally while developing 🧐  
- Put the year/month in the URL so people can link! Maybe also the map center and zoom? 🧐
- Navigation! It would be good to let people click links to other views, and put the active view in the URL.
- View data for one data source
- View data for one data source, line ref pair
- Styling! Clearly separate navigation and controls. Layout and padding. Air!💨 Make it 🎶responsive🎶
- Some sort of combobox input with completion for lineRef and data source?

Once we have feature parity, we should replace the main deployment with this app; it feels much faster as a user.