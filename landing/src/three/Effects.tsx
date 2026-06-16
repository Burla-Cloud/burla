import { useMemo } from "react";
import { Vector2 } from "three";
import {
  Bloom,
  ChromaticAberration,
  EffectComposer,
  Noise,
  Vignette,
} from "@react-three/postprocessing";
import { BlendFunction } from "postprocessing";

type Props = {
  reducedMotion: boolean;
};

export function Effects({ reducedMotion }: Props) {
  const caOffset = useMemo(() => new Vector2(0.0005, 0.0007), []);

  return (
    <EffectComposer multisampling={4}>
      <Bloom
        mipmapBlur
        intensity={1.3}
        luminanceThreshold={0.15}
        luminanceSmoothing={0.45}
        radius={0.9}
      />
      <ChromaticAberration
        blendFunction={BlendFunction.NORMAL}
        offset={caOffset}
        radialModulation={false}
        modulationOffset={0}
      />
      <Vignette eskil={false} offset={0.26} darkness={0.92} />
      <Noise
        premultiply
        blendFunction={BlendFunction.OVERLAY}
        opacity={reducedMotion ? 0 : 0.05}
      />
    </EffectComposer>
  );
}
