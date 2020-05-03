declare module 'stats' {
    export type comparator<T> = (a: T, b: T) => number;
    export function getMaxIndex<T>(input: T[], comparator: comparator<T>): number;
    export function getMinIndex<T>(input: T[], comparator: comparator<T>): number;
    export function getMedianIndex<T>(input: T[], comparator: comparator<T>): number;
    export function getMaxElement<T>(input: T[], comparator: comparator<T>): T;
    export function getMinElement<T>(input: T[], comparator: comparator<T>): T;
    export function getMedianElement<T>(input: T[], comparator: comparator<T>): T;
    export function getAverageValue<T>(input: T[], getValue: (a: T) => number): number;
}
