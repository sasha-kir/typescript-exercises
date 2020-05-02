declare module 'str-utils' {
    type StrToStr = (value: string) => string;
    
    export const strReverse: StrToStr;
    export const strToLower: StrToStr;
    export const strToUpper: StrToStr;
    export const strRandomize: StrToStr;
    export const strInvertCase: StrToStr;
}
