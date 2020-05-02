declare module 'stats' {
    type Comparator<T> = (a: T, b: T) => number;
    type ValueGetter<T> = (a: T) => T[keyof T]
    
    type IndexFn = <T>(input: T[], comparator: Comparator<T>) => number;
    type ElementFn = <T>(input: T[], comparator: Comparator<T>) => T | null;

    export const getMaxIndex: IndexFn;
    export const getMinIndex: IndexFn;
    export const getMedianIndex: IndexFn;

    export const getMaxElement: ElementFn;
    export const getMinElement: ElementFn;
    export const getMedianElement: ElementFn;

    export function getAverageValue<T>(input: T[], getValue: ValueGetter<T>): ReturnType<ValueGetter<T>> | null;
}
