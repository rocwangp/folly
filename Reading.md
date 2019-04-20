## Future::thenTry

```cpp
auto future1 = makeFuture() 创建一个空Future，Core::state是OnlyResult表示有值的Core
auto future2 = future1.then(callback1);
auto future3 = future2.then(callback2);
auto value = future3.get();
```

```cpp
auto future2 = future1.then(callback1);
```

0. 调用Future::thenImplement
    1. 函数中创建一个新的Promise和Future，分别是promise2和future2，对应的Core::state为Start
    2. 根据promise2和future2创建一个CoreCallbackState作为future1的回调函数，主要为了关联返回的promise2和回调中的state2
    3. 函数形如[state2 = CoreCallbackState(promise2, callback1)] (Try<T> t) {
           ...
           state2.setTry(callback1(t));
       }
    4. 随后调用Future::setCallback_，实际上调用的是Core::setCallback, 函数中先保存回调函数, 再检查Core::state，如果是OnlyResult，则直接调用callback
    5. 因为初始化时Core1::State是OnlyResult，所以直接调用state2.setTry(callback1(t)), state2.setTry()会调用promise2.setTry()进而调用Core2.setResult()，将结果保存，同时将state从Start转为OnlyResult

```cpp
auto future3 = future2.then(callback2);
```

0. 会调用Future::thenImplement
    1. 函数中创建一个新的Promise和Future，分别是promise3和future3，对应的Core::state为Start
    2. 根据promise2和future2创建一个CoreCallbackState作为future1的回调函数，主要为了关联返回的promise2和回调中的state2
    3. 函数形如[state2 = CoreCallbackState(promise2, callback1)] (Try<T> t) {
           ...
           state2.setTry(callback1(t));
       }

    4. 随后调用Future::setCallback_，实际上调用的是Core::setCallback, 函数中先保存回调函数, 再检查Core::state，如果是OnlyResult，则直接调用callback

    5. 因为初始化时Core1::State是OnlyResult，所以直接调用state2.setTry(callback1(t)), state2.setTry()会调用promise2.setTry()进而调用Core2.setResult()，将结果保存，同时将state从Start转为OnlyResult

0. future3.get() 会调用Future::detail::waitImpl, 函数中设置回调函数，调用setCallback(baton->post)后wait等待结果

## Future::thenError

```cpp
auto future1 = make_future();
auto future2 = future1.then([](auto&&) { throw std::logic_error{}; });
auto future3 = future2.thenError([](auto&& error) { });
```
