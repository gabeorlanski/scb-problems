export function make_counter(): Map<string, number> {
    return new Map([["x", 5], ["y", 4]]);  // Wrong count for y
}
