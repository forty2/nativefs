#include <sys/types.h>
#include <sys/stat.h>

#ifdef _WIN32

#define WIN32_LEAN_AND_MEAN
#define VC_EXTRALEAN

#include <stdio.h>
#include <io.h>
#include <windows.h> // for FlushFileBuffers

/* this is a totally hokey "implementation" of fsync, but
 * it works well enough.  Specifically, it doesn't bother
 * reporting errors since the below code doesn't check
 * for them anyway.
 */
void fsync(int fd) {
    HANDLE h = (HANDLE) _get_osfhandle(fd);

    if (h == INVALID_HANDLE_VALUE) return;

    FlushFileBuffers(h);
}

#else
#include <unistd.h>
#endif

#include <fcntl.h>
#include <string>
#include <string.h>

#include <node.h>

#ifdef _WIN32
#define open  _open
#define read  _read
#define write _write
#define close _close
#define lseek _lseeki64

#define O_RDONLY _O_RDONLY
#define O_WRONLY _O_WRONLY
#define O_CREAT  _O_CREAT
#define O_TRUNC  _O_TRUNC
#define O_BINARY _O_BINARY

// needed to support files > 4GB
#define stat __stat64
#define fstat _fstat64
#else
#define O_BINARY 0
#endif

namespace NativeFS {

using v8::Exception;
using v8::FunctionCallbackInfo;
using v8::Local;
using v8::Function;
using v8::Object;
using v8::String;
using v8::Number;
using v8::Value;

std::string get(v8::Local<v8::Value> value) {
    if (value->IsString()) {
        v8::String::Utf8Value string(value);
        return std::string(*string);
    }
    return "";
}

template <typename T>
class Property {
    public:
        virtual ~Property() {}
        virtual operator T const & () const { return value; }

        virtual T const * operator-> () const { return &value; }

    protected:
        T value;

        friend class Args;
        virtual T& operator=(const T& f) { return value = f; }
};

class StringProperty : public Property<std::string> {
    public:
        using Property<std::string>::operator->;

        virtual operator const char* () const { return value.c_str(); }

    protected:
        friend class Args;
        using Property<std::string>::operator=;
};

void ThrowError(v8::Isolate* const isolate, const char* errorText) {
    isolate->ThrowException(
        Exception::TypeError(
            String::NewFromUtf8(isolate, errorText)
        )
    );
}

class Args {
    public:
        StringProperty Source;
        StringProperty Destination;

        Property<Local<Function> > ProgressCallback;
        Property<Local<Function> > ResultCallback;

        Property<bool> UpdateProgress;
        Property<v8::Isolate*> Isolate;

        Args(const FunctionCallbackInfo<Value>& args) {
            Isolate = args.GetIsolate();

            if (args.Length() < 3) {
                ThrowError(Isolate, "Not enough arguments");
                return;
            }
            if (!args[0]->IsString()) {
                ThrowError(Isolate, "First argument is not a path");
                return;
            }
            if (!args[1]->IsString()) {
                ThrowError(Isolate, "Second argument is not a path");
                return;
            }
            if (!args[2]->IsFunction()) {
                ThrowError(Isolate, "Missing result callback");
                return;
            }
            if (args.Length() > 3 && !args[3]->IsFunction()) {
                ThrowError(Isolate, "Unknown arguments");
                return;
            }

            UpdateProgress = args.Length() > 3;

            Source      = get(args[0]);
            Destination = get(args[1]);

            if (UpdateProgress) {
                ProgressCallback = Local<Function>::Cast(args[2]);
                ResultCallback   = Local<Function>::Cast(args[3]);
            } else {
                ResultCallback   = Local<Function>::Cast(args[2]);
            }
        }
};

const int BUFFER_SIZE = 16384;

void SendProgressUpdate(const Args& args, const Local<Value>& completed, const Local<Value>& total) {
    if (args.UpdateProgress) {
        Local<Value> argv[2] = { completed, total };
        ((Local<Function>) args.ProgressCallback)->Call(Null(args.Isolate), 2, argv);
    }
}

void SendProgressUpdate(const Args& args, double completed, double total) {
    if (args.UpdateProgress) {
        SendProgressUpdate(
            args,
            Number::New(args.Isolate, (double) completed),
            Number::New(args.Isolate, (double) total)
        );
    }
}

void SendComplete(const Args& args) {
    Local<Value> argv[2] = { Null(args.Isolate), True(args.Isolate) };
    ((Local<Function>) args.ResultCallback)->Call(Null(args.Isolate), 2, argv);
}

void SendError(const Args& args, const char* errorText) {
    Local<Value> argv[2] = {
        String::NewFromUtf8(args.Isolate, errorText),
        False(args.Isolate)
    };
    ((Local<Function>) args.ResultCallback)->Call(Null(args.Isolate), 2, argv);
}

ssize_t doWrite(int fd, char *data, size_t datalen) {
    ssize_t written = write(fd, data, datalen);
    if (written == -1) return -1;
    if ((size_t) written < datalen) {
        return doWrite(fd, data + written, datalen - written);
    }
    return written;
}

void Copy(
    int fd_in, int fd_out, ssize_t inputSize,
    const Args& args, bool removeWhenDone = false
)
{
    char buffer[BUFFER_SIZE];

    const ssize_t bytesPerUpdate = inputSize / 100;

    ssize_t progress = 0;
    ssize_t bytes_read = 0;
    ssize_t sinceLastUpdate = 0;

    while ((bytes_read = read(fd_in, buffer, sizeof(buffer))) > 0)
    {
        ssize_t written = doWrite(fd_out, buffer, bytes_read);
        if (written == -1) goto copyByFdError;

        progress += bytes_read;
        sinceLastUpdate += bytes_read;

        if (sinceLastUpdate > bytesPerUpdate) {
            SendProgressUpdate(args, (double) progress, (double) inputSize);
            sinceLastUpdate = 0;
        }
    }

    if (bytes_read == -1) goto copyByFdError;

    // send one last progress update
    SendProgressUpdate(args, (double) inputSize, (double) inputSize);

    close(fd_in);

    fsync(fd_out); // Flush
    close(fd_out);

    if (removeWhenDone) {
        remove(args.Source);
    }

    SendComplete(args);
    return;

copyByFdError:
    close(fd_in);
    close(fd_out);

    remove(args.Destination); // remove failed copy
    SendError(args, strerror(errno));
    return;
}

void Copy(const FunctionCallbackInfo<Value>& incoming) {
    Args args(incoming);

    int out, in = open(args.Source, O_RDONLY | O_BINARY);
    if (in < 0) goto copyByPathError;

    struct stat st;

    // Get the input file information
    if (fstat(in, &st) != 0) {
        close(in);
        goto copyByPathError;
    }

    // Open target
    out = open(args.Destination, O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, st.st_mode);
    if (out < 0) {
        close(in);
        goto copyByPathError;
    }

    Copy(in, out, st.st_size, args);
    return;

copyByPathError:
    remove(args.Destination); // remove failed copy
    SendError(args, strerror(errno));
    return;
}

void Move(const FunctionCallbackInfo<Value>& incoming) {
    Args args(incoming);

    int out, in = open(args.Source, O_RDONLY | O_BINARY);
    if (in < 0) goto moveError;

    struct stat in_stats;

    // Get the input file information
    if (fstat(in, &in_stats) != 0) {
        close(in);
        goto moveError;
    }

    // Open target
    out = open(args.Destination, O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, in_stats.st_mode);
    if (out < 0) {
        close(in);
        goto moveError;
    }

    struct stat out_stats;

    // Get the output file information
    if (fstat(out, &out_stats) != 0) {
        close(in);
        close(out);
        goto moveError;
    }

    {
        const ssize_t inputSize = in_stats.st_size;
        if (in_stats.st_dev == out_stats.st_dev) {
            close(in);
            close(out);

            // These files are on the same device; it would
            // be much quicker to just rename the file
            remove(args.Destination);
            rename(args.Source, args.Destination);

            SendProgressUpdate(args, (double) inputSize, (double) inputSize);
            SendComplete(args);
        }
        else {
            // They're on different devices.  We'll need to
            // do this as a copy followed by a remove.
            Copy(in, out, inputSize, args, /* removeWhenDone: */ true);
        }
    }

    return;

moveError:
    remove(args.Destination); // remove failed copy
    SendError(args, strerror(errno));
    return;
}

#ifdef _WIN32
#undef open
#undef read
#undef write
#undef close

#undef O_RDONLY
#undef O_WRONLY
#undef O_CREAT
#undef O_TRUNC

#undef stat
#undef fstat
#endif

#undef O_BINARY

void init(Local<Object> exports) {
  NODE_SET_METHOD(exports, "copy", Copy);
  NODE_SET_METHOD(exports, "move", Move);
}

NODE_MODULE(native_fs, init)

}  // namespace NativeFS
