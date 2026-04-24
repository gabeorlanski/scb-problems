export function string_set(): Set<string> {
    return new Set(["a", "b", "c"]);
}

export function mixed_set(): Set<number | string> {
    return new Set([1, "two", 3]);
}
