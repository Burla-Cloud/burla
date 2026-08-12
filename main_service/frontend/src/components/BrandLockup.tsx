import { useRef } from "react";
import { Link } from "react-router-dom";

// The Burla lockup ported from burla.dev: a stair of three squares standing on
// the baseline, the wordmark in Poppins Bold, then the wide underscore block.
// The marks are shaded off the accent color so they read as beveled tiles.
// linked=false renders outside a Router (the error boundary).
export function BrandLockup({ linked = true }: { linked?: boolean }) {
    const caretRef = useRef<SVGSVGElement>(null);

    // Driven imperatively rather than off :hover so a mouse that only clips the
    // lockup in passing still gets all four blinks.
    const blink = () => {
        const caret = caretRef.current;
        if (!caret) return;
        caret.classList.remove("logo-caret-blink");
        void caret.getBoundingClientRect();
        caret.classList.add("logo-caret-blink");
    };

    const wrapperClass = "group inline-flex min-h-11 items-center text-primary text-shadow-logo";

    const lockup = (
        <span className="flex origin-left items-baseline gap-[6px] font-logo text-[23px] font-bold tracking-tight transition-transform duration-200 ease-out motion-safe:group-hover:scale-[1.03]">
                <svg viewBox="0 0 12 18" className="h-[18px] w-auto" aria-hidden="true">
                    <defs>
                        <linearGradient id="burla-face" x1="0" y1="0" x2="1" y2="1">
                            <stop offset="0" stopColor="#AEE1ED" />
                            <stop offset="0.45" stopColor="currentColor" />
                            <stop offset="1" stopColor="#56A4B7" />
                        </linearGradient>
                    </defs>
                    {[
                        [0, 0],
                        [6, 6],
                        [0, 12],
                    ].map(([x, y]) => (
                        <g key={`${x}-${y}`}>
                            <rect
                                x={x}
                                y={y + 0.5}
                                width="6"
                                height="6"
                                fill="#04141B"
                                opacity="0.36"
                            />
                            <rect x={x} y={y} width="6" height="6" fill="url(#burla-face)" />
                            <rect
                                x={x}
                                y={y}
                                width="6"
                                height="0.9"
                                fill="#EFFBFF"
                                opacity="0.28"
                            />
                        </g>
                    ))}
                </svg>
                Burla
                <svg
                    ref={caretRef}
                    viewBox="0 0 12 6"
                    className="h-[6px] w-auto"
                    aria-hidden="true"
                    onAnimationEnd={() => caretRef.current?.classList.remove("logo-caret-blink")}
                >
                    <defs>
                        <linearGradient id="burla-bar" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="0" stopColor="#AEE1ED" />
                            <stop offset="0.5" stopColor="currentColor" />
                            <stop offset="1" stopColor="#56A4B7" />
                        </linearGradient>
                    </defs>
                    <rect y="0.6" width="12" height="6" fill="#04141B" opacity="0.36" />
                    <rect width="12" height="6" fill="url(#burla-bar)" />
                </svg>
        </span>
    );

    if (!linked) {
        return (
            <span onMouseEnter={blink} onFocus={blink} className={wrapperClass}>
                {lockup}
            </span>
        );
    }

    return (
        <Link to="/" onMouseEnter={blink} onFocus={blink} className={wrapperClass}>
            {lockup}
        </Link>
    );
}
