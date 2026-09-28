# mantle logo

The mark reduces the globe the Hyperlight site draws for mantle
(`hyperlight-site/components/studies/mantle.tsx`) to a one-colour vector in the style of
the focal, slates and vorpal marks. A sphere is cut open along a seam: on one side its
stone surface, a translucent crescent crossed by contour lines; on the other, strata rings
around a solid core.

- `render-mark.py` draws all three SVGs: `python3 render-mark.py`.
- `mantle-mark.svg` is the master in `#1f2328`. `mantle-mark-light.svg` is the same file
  for light themes, and `mantle-mark-dark.svg` the same geometry in `#f0f6fc` for dark
  themes. The README shows them at 90 × 90 through a `<picture>` element.
- `mantle-mark-transparent.png` is a 1080 × 1080 export:
  `rsvg-convert -w 1080 -h 1080 mantle-mark.svg -o mantle-mark-transparent.png`.
- `mantle-mark-preview.png` shows both themes at 256, 90 and 28 pixels.

Geometry, on a 200-unit square: the sphere has radius 86; the seam is the lower-left half
of a great circle whose axis runs along the diagonal, drawn as an ellipse of half-width 36;
the rings have radii 36 to 79 and fade outward; the core has radius 24. Outlines are 6
units wide. The files hold only paths, circles, clip paths and a mask: no bitmaps, filters,
fonts, scripts or external resources.
