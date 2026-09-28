# mantle logo

The mark is the globe the Hyperlight site draws for mantle
(`hyperlight-site/components/studies/mantle.tsx`): a sphere of engraved stone, tilted a
quarter turn, with a wedge cut away to show thirty-four strata lit in a soft spectrum
around a stone core. On the site it turns slowly; here it is the first frame, the pose
the site renders before its animation starts.

- `render-globe.py` ports the component's geometry, gradients, masks and lighting to
  Python and writes `mantle-globe.svg`. Regenerate it after the site's artwork changes:
  `python3 render-globe.py > mantle-globe.svg`.
- `mantle-globe.svg` is what the README shows. Its stone and strata carry their own
  colors, so the one file reads on light and dark themes alike.
- `mantle-globe-transparent.png` is a 1080-pixel-wide export:
  `rsvg-convert -w 1080 mantle-globe.svg -o mantle-globe-transparent.png`.
- `mantle-globe-preview.png` shows it on both themes at 256, 160 and 48 pixels.

The SVG holds only vector paths, gradients, masks and clip paths: no bitmaps, filters,
fonts, scripts or external resources.
