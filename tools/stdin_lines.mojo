"""Lines from stdin, buffered properly.

`input()` returns one line and loses whatever else was already in the pipe, so
a producer reading tickets with it works only for a client that sends one and
waits — and silently drops the rest for a client that pipelines. Both tools
here are meant to be wired together with a shell pipe, where everything
arrives at once.

This reads a block at a time and hands back lines from it.
"""

from std.ffi import external_call


struct StdinLines(Movable):
    """A line reader over fd 0."""

    var buf: List[UInt8]
    var pos: Int
    var eof: Bool

    def __init__(out self):
        self.buf = List[UInt8]()
        self.pos = 0
        self.eof = False

    def __init__(out self, *, deinit move: Self):
        self.buf = move.buf^
        self.pos = move.pos
        self.eof = move.eof

    def _fill(mut self) -> Bool:
        """One block; False at end of input."""
        comptime CHUNK = 1 << 16
        var scratch = List[UInt8]()
        scratch.resize(unsafe_uninit_length=CHUNK)
        var n = external_call["read", Int](0, scratch.unsafe_ptr(), CHUNK)
        if n <= 0:
            self.eof = True
            return False
        # Drop what has already been handed out, then take the new bytes.
        if self.pos > 0:
            var rest = List[UInt8]()
            rest.extend(Span(self.buf)[self.pos : len(self.buf)])
            self.buf = rest^
            self.pos = 0
        self.buf.extend(Span(scratch)[0:n])
        return True

    def next_line(mut self) raises -> String:
        """The next line without its newline; raises at end of input."""
        while True:
            for i in range(self.pos, len(self.buf)):
                if self.buf[i] == UInt8(10):  # "\n"
                    var line = String(
                        unsafe_from_utf8=Span(self.buf)[self.pos : i]
                    )
                    self.pos = i + 1
                    return line^
            if self.eof or not self._fill():
                if self.pos < len(self.buf):
                    var tail = String(
                        unsafe_from_utf8=Span(self.buf)[
                            self.pos : len(self.buf)
                        ]
                    )
                    self.pos = len(self.buf)
                    return tail^
                raise Error("stdin: end of input")
