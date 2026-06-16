// Instanced lattice of "compute cores". Each instance lights up as an
// activation wave (uActivationRadius) expands outward from the center, then
// fades into the background fog with view-space depth so the field reads as
// effectively infinite.

export const vertexShader = /* glsl */ `
  uniform float uTime;
  uniform float uActivationRadius;
  uniform float uActiveEdge;

  attribute float aSeed;

  varying float vAct;
  varying vec3 vViewNormal;
  varying float vFogDepth;

  void main() {
    vec3 center = vec3(instanceMatrix[3][0], instanceMatrix[3][1], instanceMatrix[3][2]);
    float dist = length(center);

    float act = 1.0 - smoothstep(uActivationRadius - uActiveEdge, uActivationRadius + uActiveEdge, dist);
    float twinkle = 0.8 + 0.2 * sin(uTime * 1.5 + aSeed * 6.2831853);
    vAct = act * twinkle;

    vViewNormal = normalize(normalMatrix * normal);

    vec4 mvPosition = modelViewMatrix * instanceMatrix * vec4(position, 1.0);
    vFogDepth = -mvPosition.z;
    gl_Position = projectionMatrix * mvPosition;
  }
`;

export const fragmentShader = /* glsl */ `
  uniform vec3 uColorOff;
  uniform vec3 uColorOn;
  uniform vec3 uEmissive;
  uniform vec3 uBackground;
  uniform float uFogNear;
  uniform float uFogFar;

  varying float vAct;
  varying vec3 vViewNormal;
  varying float vFogDepth;

  void main() {
    vec3 normal = normalize(vViewNormal);
    vec3 lightDir = normalize(vec3(0.35, 0.7, 0.62));
    float diffuse = clamp(dot(normal, lightDir), 0.0, 1.0);
    float shade = 0.34 + 0.66 * diffuse;

    float act = clamp(vAct, 0.0, 1.0);
    vec3 base = mix(uColorOff, uColorOn, act) * shade;
    vec3 color = base + uEmissive * act * 2.05;

    float fog = smoothstep(uFogNear, uFogFar, vFogDepth);
    color = mix(color, uBackground, fog);

    gl_FragColor = vec4(color, 1.0);
  }
`;
