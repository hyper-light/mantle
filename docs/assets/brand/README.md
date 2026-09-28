# mantle logo

The mark reduces the globe the Hyperlight site draws for mantle
(`hyperlight-site/components/studies/mantle.tsx`) to a two-tone vector in the style of the
focal, slates and vorpal marks. A sphere is cut open along a seam: on one side its stone
surface, a solid crescent with contour lines cut through it; on the other, strata rings
around a solid core.

- `render-mark.py` draws all three SVGs: `python3 render-mark.py`.
- The colours are vorpal's. `mantle-mark.svg` is the master and `mantle-mark-light.svg` the
  light-theme file, both `#1f2328` with the crescent in `#8e9399`; `mantle-mark-dark.svg`
  uses `#f0f6fc` with the crescent in `#7c8794`. The README shows them at 90 × 90 through a
  `<picture>` element.
- `mantle-mark-transparent.png` is a 1080 × 1080 export:
  `rsvg-convert -w 1080 -h 1080 mantle-mark.svg -o mantle-mark-transparent.png`.
- `mantle-mark-preview.png` shows both themes at 256, 90 and 28 pixels.

Geometry, on a 200-unit square: the sphere has radius 86; the seam is the lower-left half
of a great circle whose axis runs along the diagonal, drawn as an ellipse of half-width 36;
the rings have radii 38, 51 and 64; the core has radius 24. Outlines are 7 units wide and
rings 6. The files hold only paths, circles, a clip path and masks: no bitmaps, filters,
fonts, scripts or external resources.
