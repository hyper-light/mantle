# mantle logo

The mark reduces the globe the Hyperlight site draws for mantle
(`hyperlight-site/components/studies/mantle.tsx`) to a two-tone vector in the style of the
focal, slates and vorpal marks. A sphere is cut open along a seam: on one side its stone
surface, a solid crescent with contour lines cut through it; on the other, strata rings
around a solid core.

- `render-mark.py` draws all three SVGs: `python3 render-mark.py`.
- Lines carry the mark and fills are shading. `mantle-mark.svg` is the master and
  `mantle-mark-light.svg` the light-theme file: lines in vorpal's mid-grey `#8e9399`, which
  stays legible if a viewer shows this file on a dark background, and fills in the same grey
  at 35% opacity. `mantle-mark-dark.svg` has lines in `#f0f6fc` and fills in `#7c8794` at 55%.
  The README shows them at 90 × 90 through a `<picture>` element.
- `mantle-mark-transparent.png` is a 1080 × 1080 export:
  `rsvg-convert -w 1080 -h 1080 mantle-mark.svg -o mantle-mark-transparent.png`.
- `mantle-mark-preview.png` shows both themes at 256, 90 and 28 pixels.

Geometry, on a 200-unit square: the sphere has radius 86; the seam is the lower-left half
of a great circle whose axis runs along the diagonal, drawn as an ellipse of half-width 36;
the rings have radii 38, 51 and 64; the core has radius 24. The outline and seam are 7
units wide; rings and the core's edge are 6. The files hold only paths, circles, a clip path and masks: no bitmaps, filters,
fonts, scripts or external resources.
