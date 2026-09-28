# mantle logo

The mark is the one the Hyperlight site draws for mantle
(`hyperlight-site/components/project-mark.tsx`), geometry unchanged: a planet of radius 12
on a 32-unit grid, its strata curving across the face at 60% opacity, and two concentric
arcs where the interior shows through. Strokes are 1.4 units with round caps and joins.

- `mantle-planet.svg` is the monochrome master; `mantle-planet-light.svg` and
  `mantle-planet-dark.svg` are the same paths stroked in `#1f2328` and `#f0f6fc`. The
  README displays these directly at 90 × 90.
- `mantle-planet-transparent.png` is a 1080 × 1080 export of the master.
- `mantle-planet-preview.png` shows both themes at 256, 90 and 28 pixels.

The files hold only vector paths: no bitmaps, filters, fonts, scripts or external
resources. Reproduce the exports from this directory with:

```sh
rsvg-convert -w 1080 -h 1080 mantle-planet.svg -o mantle-planet-transparent.png
```
